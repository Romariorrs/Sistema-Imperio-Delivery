import requests
from django.conf import settings
import re


def ensure_asaas_customer(client):
    if client.asaas_customer_id:
        return client.asaas_customer_id
    # limpa documento
    doc = client.cpf_cnpj
    doc_digits = re.sub(r"\D", "", doc or "")
    if len(doc_digits) not in (11, 14):
        raise ValueError("CPF/CNPJ inválido para criar cliente no Asaas.")
    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/customers"
    payload = {
        "name": client.name,
        "cpfCnpj": doc_digits,
        "email": client.email,
        "mobilePhone": client.phone,
    }
    headers = {"access_token": settings.ASAAS_API_KEY}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    cid = data.get("id")
    client.asaas_customer_id = cid
    client.save(update_fields=["asaas_customer_id"])
    return cid


def create_asaas_payment(client, amount, due_date):
    customer_id = ensure_asaas_customer(client)
    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/payments"
    payload = {
        "customer": customer_id,
        "billingType": "BOLETO",
        "value": float(amount),
        "dueDate": due_date.strftime("%Y-%m-%d"),
    }
    headers = {"access_token": settings.ASAAS_API_KEY}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    if not resp.ok:
        try:
            data = resp.json()
            detail = data.get("errors") or data
        except Exception:
            detail = resp.text
        raise Exception(f"Erro Asaas ao criar pagamento: {detail}")
    data = resp.json()
    return data.get("id"), data.get("invoiceUrl") or data.get("bankSlipUrl") or data.get("paymentLink")
