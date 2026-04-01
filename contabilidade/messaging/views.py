from datetime import date

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
    recurring_months = forms.IntegerField(
        required=False,
        min_value=1,
        widget=forms.NumberInput(attrs={"min": 1, "step": 1}),
    )
    select_all = forms.BooleanField(required=False, initial=False)


class SingleMessageForm(forms.Form):
    client = forms.ModelChoiceField(queryset=Client.objects.all())
    template = forms.ModelChoiceField(queryset=MessageTemplate.objects.all())
    amount = forms.DecimalField(required=False, max_digits=10, decimal_places=2)
    due_date = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))
    recurring_months = forms.IntegerField(
        required=False,
        min_value=1,
        widget=forms.NumberInput(attrs={"min": 1, "step": 1}),
    )


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


def _build_message(template, client, amount, due_date, payment_link, recurring_months):
    text = template.body
    text = text.replace("{nome}", client.name)
    text = text.replace("{valor}", f"{amount:.2f}")
    text = text.replace("{vencimento}", due_date.strftime("%d/%m/%Y"))
    text = text.replace("{link}", payment_link or "")
    text = text.replace("{meses}", str(recurring_months))
    if payment_link and "{link}" not in template.body:
        text = f"{text}\nLink de pagamento: {payment_link}"
    return text


def _create_local_billing(client, amount, due_date, recurring_months):
    checkout = create_asaas_billing(client, amount, due_date, recurring_months=recurring_months)
    return Billing.objects.create(
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


@login_required
def send_mass_messages(request):
    if request.method == "POST":
        form = MassMessageForm(request.POST)
        if form.is_valid():
            tpl = form.cleaned_data["template"]
            amount = form.cleaned_data.get("amount")
            due_date = form.cleaned_data.get("due_date") or date.today()
            recurring_months_override = form.cleaned_data.get("recurring_months")
            created = 0
            clients = Client.objects.all() if form.cleaned_data.get("select_all") else form.cleaned_data["clients"]
            for client in clients:
                if not client.phone:
                    messages.warning(request, f"Cliente {client.name} sem telefone. Nao enfileirado.")
                    continue
                amt = amount or client.default_amount
                if not amt:
                    messages.warning(request, f"Cliente {client.name} sem valor padrao.")
                    continue
                recurring_months = recurring_months_override or client.recurring_months or 1
                try:
                    billing = _create_local_billing(client, amt, due_date, recurring_months)
                except AsaasError as exc:
                    messages.error(request, f"{client.name}: {exc}")
                    continue
                final_text = _build_message(
                    tpl,
                    client,
                    amt,
                    due_date,
                    billing.payment_link,
                    billing.recurring_months,
                )
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
            recurring_months = form.cleaned_data.get("recurring_months") or client.recurring_months or 1
            if not client.phone:
                messages.error(request, f"Cliente {client.name} sem telefone. Nao enviado.")
                return redirect("send_single_message")
            try:
                billing = _create_local_billing(client, amount, due_date, recurring_months)
                payment_link = billing.payment_link
                final_text = _build_message(
                    tpl,
                    client,
                    amount,
                    due_date,
                    billing.payment_link,
                    billing.recurring_months,
                )
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
