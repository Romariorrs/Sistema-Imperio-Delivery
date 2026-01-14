from datetime import datetime

from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.shortcuts import redirect, render

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client
from contabilidade.integrations.asaas import create_asaas_payment
from contabilidade.sales.forms import SellerBillingForm, SellerClientForm
from contabilidade.sales.models import Seller
from contabilidade.messaging.models import MessageQueue
from django.db.models import Sum
from django import forms
from django.utils import timezone
from contabilidade.whatsapp.session import get_client
from contabilidade.whatsapp.services import WhatsAppError


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
        messages.error(request, "Credenciais inválidas ou usuário não é vendedor.")
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
    total_boletos = billings.count()
    valor_total = billings.aggregate(total_sum=Sum("amount"))["total_sum"] or 0
    valor_total_pago = billings_paid.aggregate(total_sum=Sum("amount"))["total_sum"] or 0

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
            # cria customer no Asaas
            try:
                from contabilidade.integrations.asaas import ensure_asaas_customer

                ensure_asaas_customer(client)
            except Exception:
                messages.warning(request, "Cliente salvo, mas não foi possível criar no Asaas agora.")
            messages.success(request, "Cliente cadastrado.")
            return redirect("seller_dashboard")
    else:
        form = SellerClientForm()
    return render(request, "sales/client_form.html", {"form": form})


@login_required
def seller_billing_create(request):
    if not hasattr(request.user, "seller_profile"):
        return redirect("dashboard")
    seller = request.user.seller_profile
    if request.method == "POST":
        form = SellerBillingForm(request.POST, seller=seller)
        if form.is_valid():
            billing = form.save(commit=False)
            client = billing.client
            try:
                payment_id, payment_link = create_asaas_payment(client, billing.amount, billing.due_date)
                billing.status = "pending"
                billing.seller = seller
                billing.asaas_billing_id = payment_id
                billing.payment_link = payment_link or ""
                billing.save()
                # Enfileira mensagem simples com link do boleto
                msg_text = (
                    f"Olá {client.name}!\n"
                    f"Sua cobrança é de R$ {billing.amount:.2f} com vencimento em {billing.due_date}.\n"
                    f"Link para pagamento: {billing.payment_link}"
                )
                MessageQueue.objects.create(
                    client=client,
                    final_text=msg_text,
                    status="pending",
                )
                # Tenta enviar imediatamente via WhatsApp (Evolution). Em caso de erro, fica enfileirada.
                try:
                    queue_item = MessageQueue.objects.filter(client=client, final_text=msg_text).latest("created_at")
                except Exception:
                    queue_item = None
                try:
                    wa_client = get_client()
                    wa_client.send_message(client.phone, msg_text)
                    if queue_item:
                        queue_item.status = "sent"
                        queue_item.sent_at = timezone.now()
                        queue_item.error_message = ""
                        queue_item.save()
                except WhatsAppError as exc:
                    if queue_item:
                        queue_item.status = "error"
                        queue_item.error_message = str(exc)
                        queue_item.sent_at = timezone.now()
                        queue_item.save()
                messages.success(
                    request,
                    "Boleto gerado. Copie o link abaixo para enviar ao cliente.",
                )
                return render(
                    request,
                    "sales/billing_confirmation.html",
                    {"billing": billing, "client": client, "payment_link": billing.payment_link},
                )
            except Exception as exc:
                messages.error(request, f"Erro ao gerar boleto: {exc}")
    else:
        form = SellerBillingForm(seller=seller)
    return render(request, "sales/billing_form.html", {"form": form})
