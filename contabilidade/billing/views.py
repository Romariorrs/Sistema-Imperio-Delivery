from datetime import datetime, date
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, render, redirect
from django.views.decorators.csrf import csrf_exempt
import json

from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue

from .models import Billing
from .services import AsaasError, create_asaas_billing


@login_required
def invoice_single_client(request, client_id):
    client = get_object_or_404(Client, pk=client_id)
    payment_link = None
    billing_obj = None

    if request.method == "POST":
        amount = Decimal(request.POST.get("amount") or client.default_amount or 0)
        due_date_raw = request.POST.get("due_date") or date.today().strftime("%Y-%m-%d")
        due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
        try:
            billing_id, payment_link = create_asaas_billing(client, amount, due_date)
            billing_obj = Billing.objects.create(
                client=client,
                amount=amount,
                due_date=due_date,
                status="pending",
                asaas_billing_id=billing_id,
                payment_link=payment_link or "",
            )
            messages.success(request, "Cobrança gerada com sucesso.")
        except AsaasError as exc:
            messages.error(request, str(exc))

    return render(
        request,
        "billing/invoice_single.html",
        {"client": client, "payment_link": payment_link, "billing": billing_obj},
    )


@csrf_exempt
def asaas_webhook(request):
    if request.method != "POST":
        return HttpResponseBadRequest("Invalid method")
    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    event = data.get("event") or data.get("type") or ""
    event_lower = event.lower()
    payment = data.get("payment") or data.get("data", {}).get("payment") or data.get("data")
    payment_id = payment.get("id") if isinstance(payment, dict) else None
    status = (payment.get("status") or "").lower() if isinstance(payment, dict) else ""

    if not payment_id:
        return HttpResponseBadRequest("Missing payment id")

    status_map = {
        "pending": "pending",
        "confirmed": "paid",
        "received": "paid",
        "received_in_cash": "paid",
        "overdue": "overdue",
        "canceled": "canceled",
        "refunded": "canceled",
    }
    new_status = status_map.get(status, None)
    # Ajusta status por evento explícito
    if "payment_deleted" in event_lower:
        new_status = "canceled"
    if "payment_received" in event_lower or "payment_confirmed" in event_lower:
        new_status = "paid"
    if "payment_overdue" in event_lower:
        new_status = "overdue"

    try:
        billing = Billing.objects.get(asaas_billing_id=payment_id)
    except Billing.DoesNotExist:
        return JsonResponse({"ok": True, "message": "payment not tracked"}, status=200)

    # Atualiza campos de valor/vencimento se vierem no webhook (payment_updated, etc.)
    update_fields = []
    if isinstance(payment, dict):
        if payment.get("value") is not None:
            billing.amount = payment["value"]
            update_fields.append("amount")
        if payment.get("dueDate"):
            try:
                billing.due_date = datetime.strptime(payment["dueDate"], "%Y-%m-%d").date()
                update_fields.append("due_date")
            except Exception:
                pass
        link = payment.get("invoiceUrl") or payment.get("bankSlipUrl") or payment.get("paymentLink")
        if link:
            billing.payment_link = link
            update_fields.append("payment_link")

    if new_status:
        billing.status = new_status
        update_fields.append("status")

    if update_fields:
        update_fields.append("updated_at")
        billing.save(update_fields=list(set(update_fields)))

    return JsonResponse({"ok": True, "event": event, "status": status})


@login_required
@user_passes_test(lambda u: u.is_staff)
def reset_finance_view(request):
    if request.method == "POST":
        Billing.objects.all().delete()
        MessageQueue.objects.all().delete()
        messages.success(request, "Financeiro zerado: cobranças e fila limpas.")
        return redirect("dashboard")
    return HttpResponseBadRequest("Invalid method")
