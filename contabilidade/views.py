import calendar
import io
from datetime import timedelta
import requests

from django.contrib.auth.decorators import login_required
from django.contrib.auth.decorators import user_passes_test
from django.contrib import messages
from decimal import Decimal

from django.db.models import Count, Q, Sum
from django.shortcuts import render, redirect
from django.core.management import call_command
from django.utils import timezone

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue
from contabilidade.sales.models import Seller


@login_required
def dashboard(request):
    if hasattr(request.user, "seller_profile") and not (request.user.is_staff or request.user.is_superuser):
        return redirect("seller_dashboard")
    total_sent = MessageQueue.objects.filter(status="sent").count()
    total_pending_queue = MessageQueue.objects.filter(status="pending").count()
    total_error_queue = MessageQueue.objects.filter(status="error").count()
    total_to_receive_local = (
        Billing.objects.filter(status__in=["pending", "overdue"]).aggregate(
            total=Sum("amount")
        )["total"]
        or 0
    )
    total_received_local = (
        Billing.objects.filter(status="paid").aggregate(total=Sum("amount"))["total"]
        or 0
    )

    total_received_display = total_received_local
    total_to_receive_display = total_to_receive_local
    # Alertas (IA interna simplificada)
    alerts = []
    pending_count = Billing.objects.filter(status="pending").count()
    overdue_count = Billing.objects.filter(status="overdue").count()
    queue_pending = MessageQueue.objects.exclude(status="sent").count()
    missing_asaas = Client.objects.filter(asaas_customer_id__isnull=True).count() + Client.objects.filter(
        asaas_customer_id=""
    ).count()

    if overdue_count > 0:
        alerts.append(
            {
                "title": "Cobranças em atraso",
                "detail": f"{overdue_count} cobrança(s) atrasada(s). Considere reenviar lembrete.",
                "level": "warning",
            }
        )
    if pending_count > 0:
        alerts.append(
            {
                "title": "Cobranças pendentes",
                "detail": f"{pending_count} cobrança(s) aguardando pagamento.",
                "level": "info",
            }
        )
    if queue_pending > 0:
        alerts.append(
            {
                "title": "Fila de mensagens",
                "detail": f"{queue_pending} mensagem(ns) não enviadas na fila WhatsApp.",
                "level": "info",
            }
        )
    if missing_asaas > 0:
        alerts.append(
            {
                "title": "Clientes sem Asaas ID",
                "detail": f"{missing_asaas} cliente(s) sem asaas_customer_id. Cadastre para gerar cobranças.",
                "level": "warning",
            }
        )
    if not alerts:
        alerts.append({"title": "Tudo em dia", "detail": "Nenhum alerta no momento.", "level": "success"})
    overdue_clients = (
        Billing.objects.filter(status="overdue")
        .select_related("client")
        .order_by("-due_date")
    )
    # Comissoes por vendedor
    seller_stats = (
        Billing.objects.filter(seller__isnull=False)
        .values("seller")
        .annotate(
            total_sum=Sum("amount"),
            total_count=Count("id"),
            paid_sum=Sum("amount", filter=Q(status="paid")),
            paid_count=Count("id", filter=Q(status="paid")),
            pending_sum=Sum("amount", filter=Q(status__in=["pending", "overdue"])),
            pending_count=Count("id", filter=Q(status__in=["pending", "overdue"])),
        )
    )
    stats_by_seller = {row["seller"]: row for row in seller_stats}
    seller_commissions = []
    for seller in Seller.objects.filter(active=True).order_by("name"):
        stats = stats_by_seller.get(seller.id, {})
        paid_sum = stats.get("paid_sum") or Decimal("0")
        paid_count = stats.get("paid_count") or 0
        pending_sum = stats.get("pending_sum") or Decimal("0")
        pending_count = stats.get("pending_count") or 0
        if seller.commission_type == "FIXED":
            commission_due = seller.commission_value * paid_count
            commission_pending = seller.commission_value * pending_count
        else:
            commission_due = (paid_sum * seller.commission_value) / Decimal("100")
            commission_pending = (pending_sum * seller.commission_value) / Decimal("100")
        seller_commissions.append(
            {
                "seller": seller,
                "total_sum": stats.get("total_sum") or Decimal("0"),
                "total_count": stats.get("total_count") or 0,
                "paid_sum": paid_sum,
                "paid_count": paid_count,
                "commission_due": commission_due,
                "commission_pending": commission_pending,
            }
        )
    context = {
        "total_sent": total_sent,
        "total_pending_queue": total_pending_queue,
        "total_error_queue": total_error_queue,
        "total_to_receive": total_to_receive_display,
        "total_received": total_received_display,
        "overdue_clients": overdue_clients,
        "clients": Client.objects.all()[:5],
        "alerts": alerts,
        "seller_commissions": seller_commissions,
    }
    return render(request, "dashboard.html", context)


def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _add_one_month(value):
    year = value.year + (value.month // 12)
    month = 1 if value.month == 12 else value.month + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(value.day, last_day)
    return value.replace(year=year, month=month, day=day)


@login_required
@user_passes_test(lambda u: u.is_staff or u.is_superuser)
def run_monthly_billing_view(request):
    if request.method != "POST":
        return redirect("dashboard")

    action = request.POST.get("action") or "generate"
    create_new = request.POST.get("create_new") == "on"
    send_reminders = request.POST.get("send_reminders") == "on"

    if action == "send_queue":
        create_new = False
        send_reminders = False

    days_ahead = _safe_int(request.POST.get("days_ahead"), 30)
    remind_days = _safe_int(request.POST.get("remind_days"), 2)
    cooldown_days = _safe_int(request.POST.get("cooldown_days"), 2)
    delay = _safe_float(request.POST.get("delay"), 4.0)
    send_now = action in {"generate_send", "send_queue"}

    out = io.StringIO()
    try:
        call_command(
            "run_monthly_billing",
            days_ahead=days_ahead,
            remind_days=remind_days,
            cooldown_days=cooldown_days,
            no_create=not create_new,
            no_remind=not send_reminders,
            send_now=send_now,
            delay=delay,
            stdout=out,
        )
        result = out.getvalue().strip()
        if result:
            messages.success(request, result)
        else:
            messages.success(request, "Rotina concluida.")
    except Exception as exc:
        messages.error(request, f"Erro ao executar rotina: {exc}")
    return redirect("dashboard")


@login_required
@user_passes_test(lambda u: u.is_staff or u.is_superuser)
def monthly_billing_page(request):
    today = timezone.localdate()
    due_warning_date = today + timedelta(days=2)
    month_start = today.replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_end = next_month - timedelta(days=1)

    month_billings = Billing.objects.filter(due_date__range=(month_start, month_end))
    paid_clients = (
        month_billings.filter(status="paid")
        .values("client_id")
        .distinct()
        .count()
    )
    pending_clients = (
        month_billings.filter(status__in=["pending", "overdue"])
        .values("client_id")
        .distinct()
        .count()
    )
    overdue_clients = (
        Billing.objects.filter(status="overdue")
        .values("client_id")
        .distinct()
        .count()
    )

    queue_pending = MessageQueue.objects.filter(status="pending").count()
    queue_error = MessageQueue.objects.filter(status="error").count()

    due_soon_list = []
    clients = Client.objects.filter(active=True).order_by("name")
    for client in clients:
        if not client.default_amount or client.default_amount <= 0:
            continue
        last_billing = Billing.objects.filter(client=client).order_by("-due_date").first()
        if last_billing and last_billing.due_date >= today and last_billing.status != "canceled":
            next_due = last_billing.due_date
        else:
            if last_billing:
                next_due = _add_one_month(last_billing.due_date)
            else:
                next_due = client.created_at.date() + timedelta(days=30)
            if next_due < today:
                next_due = today
        if next_due == due_warning_date:
            due_soon_list.append({"client": client, "due_date": next_due})

    context = {
        "month_label": today.strftime("%m/%Y"),
        "due_soon_clients": len(due_soon_list),
        "due_soon_list": due_soon_list,
        "paid_clients": paid_clients,
        "pending_clients": pending_clients,
        "overdue_clients_count": overdue_clients,
        "queue_pending": queue_pending,
        "queue_error": queue_error,
    }
    return render(request, "billing/monthly.html", context)
