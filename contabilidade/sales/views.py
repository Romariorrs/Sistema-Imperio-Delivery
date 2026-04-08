import re

from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.db.models import Sum
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone

from contabilidade.billing.models import Billing
from contabilidade.billing.services import AsaasError, create_asaas_billing, ensure_asaas_customer
from contabilidade.messaging.models import MessageQueue
from contabilidade.sales.forms import SellerBillingForm, SellerClientForm
from contabilidade.sales.models import SellerLeadAssignment
from contabilidade.whatsapp.services import WhatsAppError
from contabilidade.whatsapp.session import get_client


def _build_seller_message(client, billing):
    return (
        f"Ola {client.name}!\n"
        f"Sua cobranca recorrente no cartao foi criada.\n"
        f"Valor mensal: R$ {billing.amount:.2f}\n"
        f"Dia da compra: {billing.due_date.strftime('%d/%m/%Y')}\n"
        f"Duracao: {billing.recurring_months} mes(es)\n"
        f"Link para concluir o cadastro do cartao: {billing.payment_link}"
    )


def _send_queue_item_now(queue_item: MessageQueue):
    try:
        wa_client = get_client()
        wa_client.send_message(queue_item.client.phone, queue_item.final_text)
        queue_item.status = "sent"
        queue_item.sent_at = timezone.now()
        queue_item.error_message = ""
    except WhatsAppError as exc:
        queue_item.status = "error"
        queue_item.error_message = str(exc)
        queue_item.sent_at = timezone.now()
    queue_item.attempts += 1
    queue_item.save(update_fields=["status", "sent_at", "error_message", "attempts"])


def _current_seller_assignment(seller):
    current = (
        seller.lead_assignments.select_related("macro_lead")
        .filter(status="viewed")
        .order_by("sequence", "id")
        .first()
    )
    if current:
        return current

    current = (
        seller.lead_assignments.select_related("macro_lead")
        .filter(status="pending")
        .order_by("sequence", "id")
        .first()
    )
    if not current:
        return None

    current.status = "viewed"
    if current.first_viewed_at is None:
        current.first_viewed_at = timezone.now()
        current.save(update_fields=["status", "first_viewed_at"])
    else:
        current.save(update_fields=["status"])
    return current


def _lead_phone_digits(lead):
    digits = re.sub(r"\D", "", lead.representative_phone_norm or lead.representative_phone or "")
    if digits and not digits.startswith("55") and len(digits) in (10, 11):
        digits = f"55{digits}"
    return digits


def seller_login(request):
    if request.user.is_authenticated and hasattr(request.user, "seller_profile"):
        return redirect("seller_dashboard")

    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        user = authenticate(request, username=username, password=password)
        if user is not None and hasattr(user, "seller_profile"):
            login(request, user)
            return redirect("seller_dashboard")
        messages.error(request, "Credenciais invalidas ou usuario nao e vendedor.")
    return render(request, "sales/login.html")


def seller_logout(request):
    logout(request)
    return redirect("seller_login")


@login_required
def seller_dashboard(request):
    if not hasattr(request.user, "seller_profile"):
        return redirect("dashboard")
    seller = request.user.seller_profile
    billings = Billing.objects.filter(seller=seller)
    billings_paid = billings.filter(status="paid")
    lead_assignments = seller.lead_assignments.all()
    total_boletos = billings.count()
    valor_total = billings.aggregate(total_sum=Sum("amount"))["total_sum"] or 0
    valor_total_pago = billings_paid.aggregate(total_sum=Sum("amount"))["total_sum"] or 0
    leads_remaining = lead_assignments.filter(status__in=SellerLeadAssignment.ACTIVE_STATUSES).count()
    leads_completed = lead_assignments.filter(status="completed").count()

    if seller.commission_type == "FIXED":
        comissao_estimada = seller.commission_value * billings_paid.count()
    else:
        comissao_estimada = sum(
            [b.amount * seller.commission_value / 100 for b in billings_paid if b.amount]
        )

    context = {
        "seller": seller,
        "total_boletos": total_boletos,
        "valor_total": valor_total,
        "valor_total_pago": valor_total_pago,
        "comissao_estimada": comissao_estimada,
        "leads_remaining": leads_remaining,
        "leads_completed": leads_completed,
    }
    return render(request, "sales/dashboard.html", context)


@login_required
def seller_client_create(request):
    if not hasattr(request.user, "seller_profile"):
        return redirect("dashboard")
    seller = request.user.seller_profile
    if request.method == "POST":
        form = SellerClientForm(request.POST)
        if form.is_valid():
            client = form.save(commit=False)
            client.created_by = seller
            client.save()
            try:
                ensure_asaas_customer(client)
            except AsaasError as exc:
                messages.warning(request, f"Cliente salvo, mas o Asaas retornou: {exc}")
            messages.success(request, "Cliente cadastrado.")
            return redirect(f"{reverse('seller_billing_create')}?client={client.id}")
    else:
        form = SellerClientForm()
    return render(request, "sales/client_form.html", {"form": form})


@login_required
def seller_billing_create(request):
    if not hasattr(request.user, "seller_profile"):
        return redirect("dashboard")
    seller = request.user.seller_profile
    initial_client = request.GET.get("client")

    if request.method == "POST":
        form = SellerBillingForm(request.POST, seller=seller)
        if form.is_valid():
            billing = form.save(commit=False)
            client = billing.client
            try:
                checkout = create_asaas_billing(
                    client,
                    billing.amount,
                    billing.due_date,
                    recurring_months=billing.recurring_months,
                )
                client.default_amount = billing.amount
                client.recurring_months = billing.recurring_months
                client.save(update_fields=["default_amount", "recurring_months", "updated_at"])

                billing.status = "pending"
                billing.seller = seller
                billing.billing_type = checkout["billing_type"]
                billing.charge_type = checkout["charge_type"]
                billing.subscription_end_date = checkout["subscription_end_date"]
                billing.asaas_checkout_id = checkout["checkout_id"]
                billing.payment_link = checkout["payment_link"] or ""
                billing.save()

                queue_item = MessageQueue.objects.create(
                    client=client,
                    billing=billing,
                    final_text=_build_seller_message(client, billing),
                    status="pending",
                )
                _send_queue_item_now(queue_item)

                messages.success(
                    request,
                    "Checkout recorrente gerado. O link foi preparado para envio via Evolution API.",
                )
                return render(
                    request,
                    "sales/billing_confirmation.html",
                    {"billing": billing, "client": client, "payment_link": billing.payment_link},
                )
            except AsaasError as exc:
                messages.error(request, f"Erro ao gerar cobranca recorrente: {exc}")
    else:
        form = SellerBillingForm(seller=seller, initial_client=initial_client)
    return render(request, "sales/billing_form.html", {"form": form})


@login_required
def seller_leads(request):
    if not hasattr(request.user, "seller_profile"):
        return redirect("dashboard")

    seller = request.user.seller_profile
    if request.method == "POST":
        assignment = (
            seller.lead_assignments.select_related("macro_lead")
            .filter(id=request.POST.get("assignment_id"))
            .first()
        )
        action = (request.POST.get("action") or "").strip().lower()
        if not assignment:
            messages.error(request, "Lead atual nao encontrado.")
            return redirect("seller_leads")
        if assignment.status not in SellerLeadAssignment.ACTIVE_STATUSES:
            messages.error(request, "Esse lead ja foi finalizado.")
            return redirect("seller_leads")
        if action not in {"complete", "next"}:
            messages.error(request, "Acao invalida.")
            return redirect("seller_leads")

        assignment.status = "completed" if action == "complete" else "skipped"
        assignment.completed_at = timezone.now()
        assignment.save(update_fields=["status", "completed_at"])
        return redirect("seller_leads")

    current_assignment = _current_seller_assignment(seller)
    lead = current_assignment.macro_lead if current_assignment else None
    remaining_count = seller.lead_assignments.filter(
        status__in=SellerLeadAssignment.ACTIVE_STATUSES
    ).count()
    completed_count = seller.lead_assignments.filter(status="completed").count()
    skipped_count = seller.lead_assignments.filter(status="skipped").count()
    total_count = seller.lead_assignments.count()
    whatsapp_digits = _lead_phone_digits(lead) if lead else ""

    context = {
        "seller": seller,
        "current_assignment": current_assignment,
        "lead": lead,
        "remaining_count": remaining_count,
        "completed_count": completed_count,
        "skipped_count": skipped_count,
        "total_count": total_count,
        "whatsapp_url": f"https://wa.me/{whatsapp_digits}" if whatsapp_digits else "",
    }
    return render(request, "sales/leads.html", context)
