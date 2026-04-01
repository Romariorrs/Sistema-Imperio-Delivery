import json
from datetime import date, datetime
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponseBadRequest, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_exempt

from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue

from .models import Billing
from .services import AsaasError, create_asaas_billing


def _coerce_payload(data, key):
    payload = data.get(key)
    if isinstance(payload, dict):
        return payload
    nested = data.get("data")
    if isinstance(nested, dict):
        value = nested.get(key)
        if isinstance(value, dict):
            return value
        if key == "payment" and nested.get("object") == "payment":
            return nested
        if key == "checkout" and nested.get("object") == "checkout":
            return nested
    return {}


@login_required
def invoice_single_client(request, client_id):
    client = get_object_or_404(Client, pk=client_id)
    payment_link = None
    billing_obj = None

    if request.method == "POST":
        amount = Decimal(request.POST.get("amount") or client.default_amount or 0)
        recurring_months = int(request.POST.get("recurring_months") or client.recurring_months or 1)
        due_date_raw = request.POST.get("due_date") or date.today().strftime("%Y-%m-%d")
        due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
        try:
            checkout = create_asaas_billing(
                client,
                amount,
                due_date,
                recurring_months=recurring_months,
            )
            billing_obj = Billing.objects.create(
                client=client,
                amount=amount,
                due_date=due_date,
                subscription_end_date=checkout["subscription_end_date"],
                recurring_months=checkout["recurring_months"],
                status="pending",
                billing_type=checkout["billing_type"],
                charge_type=checkout["charge_type"],
                asaas_checkout_id=checkout["checkout_id"],
                payment_link=checkout["payment_link"] or "",
            )
            payment_link = billing_obj.payment_link
            messages.success(request, "Checkout recorrente gerado com sucesso.")
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

    event = (data.get("event") or data.get("type") or "").upper()

    if event.startswith("CHECKOUT_"):
        checkout = _coerce_payload(data, "checkout")
        checkout_id = checkout.get("id")
        if not checkout_id:
            return HttpResponseBadRequest("Missing checkout id")

        billing = Billing.objects.filter(asaas_checkout_id=checkout_id).first()
        if not billing:
            return JsonResponse({"ok": True, "message": "checkout not tracked"}, status=200)

        update_fields = []
        if event == "CHECKOUT_PAID":
            billing.status = "paid"
            update_fields.append("status")
        elif event in {"CHECKOUT_CANCELED", "CHECKOUT_EXPIRED"}:
            billing.status = "canceled"
            update_fields.append("status")
        elif event == "CHECKOUT_CREATED" and billing.status != "pending":
            billing.status = "pending"
            update_fields.append("status")

        if update_fields:
            update_fields.append("updated_at")
            billing.save(update_fields=list(set(update_fields)))
        return JsonResponse({"ok": True, "event": event, "checkout": checkout_id})

    payment = _coerce_payload(data, "payment")
    payment_id = payment.get("id")
    if not payment_id:
        return HttpResponseBadRequest("Missing payment id")

    status = (payment.get("status") or "").lower()
    status_map = {
        "pending": "pending",
        "confirmed": "paid",
        "received": "paid",
        "received_in_cash": "paid",
        "overdue": "overdue",
        "canceled": "canceled",
        "refunded": "canceled",
    }
    new_status = status_map.get(status)
    if event in {"PAYMENT_RECEIVED", "PAYMENT_CONFIRMED"}:
        new_status = "paid"
    elif event == "PAYMENT_OVERDUE":
        new_status = "overdue"
    elif event == "PAYMENT_DELETED":
        new_status = "canceled"

    billing = Billing.objects.filter(asaas_billing_id=payment_id).first()
    if not billing:
        subscription_id = payment.get("subscription")
        checkout_session = payment.get("checkoutSession")
        if subscription_id:
            billing = Billing.objects.filter(asaas_subscription_id=subscription_id).order_by("-created_at").first()
        if not billing and checkout_session:
            billing = Billing.objects.filter(asaas_checkout_id=checkout_session).first()
    if not billing:
        return JsonResponse({"ok": True, "message": "payment not tracked"}, status=200)

    update_fields = []
    if payment.get("value") is not None:
        billing.amount = payment["value"]
        update_fields.append("amount")
    if payment.get("dueDate"):
        try:
            billing.due_date = datetime.strptime(payment["dueDate"], "%Y-%m-%d").date()
            update_fields.append("due_date")
        except Exception:
            pass

    link = (
        payment.get("invoiceUrl")
        or payment.get("bankSlipUrl")
        or payment.get("paymentLink")
        or payment.get("transactionReceiptUrl")
    )
    if link:
        billing.payment_link = link
        update_fields.append("payment_link")

    billing_type = payment.get("billingType")
    if billing_type and billing.billing_type != billing_type:
        billing.billing_type = billing_type
        update_fields.append("billing_type")

    if payment_id and billing.asaas_billing_id != payment_id:
        billing.asaas_billing_id = payment_id
        update_fields.append("asaas_billing_id")

    subscription_id = payment.get("subscription")
    if subscription_id and billing.asaas_subscription_id != subscription_id:
        billing.asaas_subscription_id = subscription_id
        update_fields.append("asaas_subscription_id")

    if new_status and billing.status != new_status:
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
        messages.success(request, "Financeiro zerado: cobrancas e fila limpas.")
        return redirect("dashboard")
    return HttpResponseBadRequest("Invalid method")
