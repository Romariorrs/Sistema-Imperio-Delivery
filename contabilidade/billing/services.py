import requests
from django.conf import settings

from contabilidade.clients.models import Client


class AsaasError(Exception):
    pass


def _asaas_headers():
    if not settings.ASAAS_API_KEY:
        raise AsaasError("Chave ASAAS_API_KEY não configurada. Defina no .env ou variável de ambiente.")
    return {
        "access_token": settings.ASAAS_API_KEY,
        "Content-Type": "application/json",
    }


def create_asaas_customer(client: Client):
    """Cria um customer no Asaas e salva no registro do cliente."""
    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/customers"
    payload = {
        "name": client.name,
        "cpfCnpj": client.cpf_cnpj,
        "mobilePhone": client.phone or None,
        "email": client.email or None,
    }
    response = requests.post(url, json=payload, headers=_asaas_headers(), timeout=30)
    if not response.ok:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        snippet = str(detail)
        if len(snippet) > 400:
            snippet = snippet[:400] + "..."
        raise AsaasError(f"Erro ao criar cliente no Asaas (HTTP {response.status_code}) detalhe={snippet}")
    data = response.json()
    customer_id = data.get("id")
    if not customer_id:
        raise AsaasError("Asaas não retornou ID do cliente.")
    client.asaas_customer_id = customer_id
    client.save(update_fields=["asaas_customer_id"])
    return customer_id


def create_asaas_billing(client, amount, due_date):
    customer_id = client.asaas_customer_id or settings.ASAAS_CUSTOMER_DEFAULT_ID
    created_customer = False
    if not customer_id:
        customer_id = create_asaas_customer(client)
        created_customer = True

    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/payments"
    payload = {
        "customer": customer_id,
        "billingType": "PIX",
        "value": float(amount),
        "dueDate": due_date.strftime("%Y-%m-%d"),
    }
    headers = _asaas_headers()

    def _do_request():
        return requests.post(url, json=payload, headers=headers, timeout=30)

    response = _do_request()
    if not response.ok:
        # Se customer for inválido, cria e tenta 1 vez
        if response.status_code == 404 and not created_customer:
            customer_id = create_asaas_customer(client)
            payload["customer"] = customer_id
            response = _do_request()

    if not response.ok:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        snippet = str(detail)
        if len(snippet) > 400:
            snippet = snippet[:400] + "..."
        raise AsaasError(
            f"Erro ao criar cobrança (HTTP {response.status_code}) "
            f"URL={url} payload={payload} detalhe={snippet}"
        )

    data = response.json()
    billing_id = data.get("id")
    payment_link = (
        data.get("invoiceUrl")
        or data.get("bankSlipUrl")
        or data.get("paymentLink")
        or data.get("invoicePdfUrl")
        or data.get("transactionReceiptUrl")
    )
    if not billing_id:
        raise AsaasError("Falha ao criar cobrança no Asaas.")
    return billing_id, payment_link
