import csv

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django import forms
from django.views.decorators.http import require_POST

from contabilidade.integrations.asaas import ensure_asaas_customer
from .models import Client


class ClientForm(forms.ModelForm):
    class Meta:
        model = Client
        fields = [
            "name",
            "cpf_cnpj",
            "phone",
            "email",
            "default_amount",
            "active",
            "asaas_customer_id",
        ]


@login_required
def client_list(request):
    clients = Client.objects.all().order_by("-created_at")
    return render(request, "clients/list.html", {"clients": clients})


@login_required
def client_create(request):
    if request.method == "POST":
        form = ClientForm(request.POST)
        if form.is_valid():
            client = form.save()
            try:
                ensure_asaas_customer(client)
            except Exception:
                pass
            return redirect("client_list")
    else:
        form = ClientForm()
    return render(request, "clients/form.html", {"form": form, "title": "Novo Cliente"})


@login_required
def client_update(request, pk):
    client = get_object_or_404(Client, pk=pk)
    if request.method == "POST":
        form = ClientForm(request.POST, instance=client)
        if form.is_valid():
            client = form.save()
            try:
                ensure_asaas_customer(client)
            except Exception:
                pass
            return redirect("client_list")
    else:
        form = ClientForm(instance=client)
    return render(request, "clients/form.html", {"form": form, "title": "Editar Cliente"})


@login_required
@require_POST
def client_delete(request, pk):
    client = get_object_or_404(Client, pk=pk)
    client.delete()
    messages.success(request, "Cliente excluido.")
    return redirect("client_list")


@login_required
def client_import(request):
    return render(request, "clients/import.html")


@login_required
def client_export(request):
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="clientes.csv"'
    writer = csv.writer(response)
    writer.writerow(
        ["Nome", "CPF/CNPJ", "Telefone", "Email", "Valor Padrão", "Ativo", "Asaas ID", "Criado em"]
    )
    for client in Client.objects.all():
        writer.writerow(
            [
                client.name,
                client.cpf_cnpj,
                client.phone,
                client.email,
                client.default_amount,
                "Sim" if client.active else "Não",
                client.asaas_customer_id,
                client.created_at.strftime("%Y-%m-%d"),
            ]
        )
    return response
