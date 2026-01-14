from datetime import date, datetime

from django import forms
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, redirect, render

from contabilidade.billing.models import Billing
from contabilidade.billing.services import AsaasError, create_asaas_billing
from contabilidade.clients.models import Client

from .models import MessageQueue, MessageTemplate


class TemplateForm(forms.ModelForm):
    class Meta:
        model = MessageTemplate
        fields = ["name", "body"]


class MassMessageForm(forms.Form):
    clients = forms.ModelMultipleChoiceField(queryset=Client.objects.all(), required=False)
    template = forms.ModelChoiceField(queryset=MessageTemplate.objects.all())
    amount = forms.DecimalField(required=False, max_digits=10, decimal_places=2)
    due_date = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))
    select_all = forms.BooleanField(required=False, initial=False)


class SingleMessageForm(forms.Form):
    client = forms.ModelChoiceField(queryset=Client.objects.all())
    template = forms.ModelChoiceField(queryset=MessageTemplate.objects.all())
    amount = forms.DecimalField(required=False, max_digits=10, decimal_places=2)
    due_date = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))


@login_required
def template_list(request):
    templates = MessageTemplate.objects.all()
    return render(request, "messaging/template_list.html", {"templates": templates})


@login_required
def template_create(request):
    if request.method == "POST":
        form = TemplateForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Template criado.")
            return redirect("template_list")
    else:
        form = TemplateForm()
    return render(request, "messaging/template_form.html", {"form": form, "title": "Novo Template"})


@login_required
def template_update(request, pk):
    template = get_object_or_404(MessageTemplate, pk=pk)
    if request.method == "POST":
        form = TemplateForm(request.POST, instance=template)
        if form.is_valid():
            form.save()
            messages.success(request, "Template atualizado.")
            return redirect("template_list")
    else:
        form = TemplateForm(instance=template)
    return render(request, "messaging/template_form.html", {"form": form, "title": "Editar Template"})


def _build_message(template, client, amount, due_date, payment_link):
    text = template.body
    text = text.replace("{nome}", client.name)
    text = text.replace("{valor}", f"{amount:.2f}")
    text = text.replace("{vencimento}", due_date.strftime("%d/%m/%Y"))
    text = text.replace("{link}", payment_link or "")
    # Se o template não tiver placeholder e existir link, acrescenta no final.
    if payment_link and "{link}" not in template.body:
        text = f"{text}\nLink de pagamento: {payment_link}"
    return text


@login_required
def send_mass_messages(request):
    if request.method == "POST":
        form = MassMessageForm(request.POST)
        if form.is_valid():
            tpl = form.cleaned_data["template"]
            amount = form.cleaned_data.get("amount")
            due_date = form.cleaned_data.get("due_date") or date.today()
            created = 0
            clients = Client.objects.all() if form.cleaned_data.get("select_all") else form.cleaned_data["clients"]
            for client in clients:
                if not client.phone:
                    messages.warning(request, f"Cliente {client.name} sem telefone. Não enfileirado.")
                    continue
                if not client.asaas_customer_id:
                    messages.warning(request, f"Cliente {client.name} sem Asaas ID. Não enfileirado.")
                    continue
                amt = amount or client.default_amount
                if not amt:
                    messages.warning(request, f"Cliente {client.name} sem valor padrão.")
                    continue
                try:
                    billing_id, link = create_asaas_billing(client, amt, due_date)
                    billing = Billing.objects.create(
                        client=client,
                        amount=amt,
                        due_date=due_date,
                        status="pending",
                        asaas_billing_id=billing_id,
                        payment_link=link or "",
                    )
                except AsaasError as exc:
                    messages.error(request, f"{client.name}: {exc}")
                    continue
                final_text = _build_message(tpl, client, amt, due_date, billing.payment_link)
                MessageQueue.objects.create(
                    client=client,
                    billing=billing,
                    template=tpl,
                    final_text=final_text,
                )
                created += 1
            messages.success(request, f"Mensagens enfileiradas: {created}.")
            return redirect("send_mass_messages")
    else:
        form = MassMessageForm()

    queue = MessageQueue.objects.order_by("-created_at")[:20]
    return render(request, "messaging/send_mass.html", {"form": form, "queue": queue})


@login_required
def send_single_message(request):
    payment_link = None
    billing = None
    if request.method == "POST":
        form = SingleMessageForm(request.POST)
        if form.is_valid():
            client = form.cleaned_data["client"]
            tpl = form.cleaned_data["template"]
            amount = form.cleaned_data.get("amount") or client.default_amount
            due_date = form.cleaned_data.get("due_date") or date.today()
            if not client.phone:
                messages.error(request, f"Cliente {client.name} sem telefone. Não enviado.")
                return redirect("send_single_message")
            if not client.asaas_customer_id:
                messages.error(request, f"Cliente {client.name} sem Asaas ID. Não enviado.")
                return redirect("send_single_message")
            try:
                billing_id, payment_link = create_asaas_billing(client, amount, due_date)
                billing = Billing.objects.create(
                    client=client,
                    amount=amount,
                    due_date=due_date,
                    status="pending",
                    asaas_billing_id=billing_id,
                    payment_link=payment_link or "",
                )
                final_text = _build_message(tpl, client, amount, due_date, billing.payment_link)
                MessageQueue.objects.create(
                    client=client,
                    billing=billing,
                    template=tpl,
                    final_text=final_text,
                )
                messages.success(request, "Mensagem enfileirada para envio.")
            except AsaasError as exc:
                messages.error(request, str(exc))
    else:
        form = SingleMessageForm()

    return render(
        request,
        "messaging/send_single.html",
        {"form": form, "payment_link": payment_link, "billing": billing},
    )


@login_required
def queue_delete(request, pk):
    if request.method == "POST":
        item = get_object_or_404(MessageQueue, pk=pk)
        item.delete()
    return redirect("send_mass_messages")


@login_required
def queue_clear(request):
    if request.method == "POST":
        MessageQueue.objects.all().delete()
    return redirect("send_mass_messages")
