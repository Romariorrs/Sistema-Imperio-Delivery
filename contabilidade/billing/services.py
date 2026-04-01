import calendar
import re
from datetime import date

import requests
from django.conf import settings

from contabilidade.clients.models import Client


class AsaasError(Exception):
    pass


def _add_months(value: date, months: int) -> date:
    total_month = value.month - 1 + months
    year = value.year + (total_month // 12)
    month = (total_month % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(value.day, last_day)
    return date(year, month, day)


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _asaas_headers():
    if not settings.ASAAS_API_KEY:
        raise AsaasError("Chave ASAAS_API_KEY nao configurada. Defina no .env ou variavel de ambiente.")
    return {
        "access_token": settings.ASAAS_API_KEY,
        "Content-Type": "application/json",
    }


def create_asaas_customer(client: Client):
    """Cria um customer no Asaas e salva no registro do cliente."""
    cpf_cnpj = _digits_only(client.cpf_cnpj)
    if len(cpf_cnpj) not in {11, 14}:
        raise AsaasError("CPF/CNPJ invalido para criar cliente no Asaas.")

    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/customers"
    payload = {
        "name": client.name,
        "cpfCnpj": cpf_cnpj,
        "mobilePhone": _digits_only(client.phone) or None,
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
        raise AsaasError("Asaas nao retornou ID do cliente.")

    client.asaas_customer_id = customer_id
    client.save(update_fields=["asaas_customer_id"])
    return customer_id


def ensure_asaas_customer(client: Client):
    if client.asaas_customer_id:
        return client.asaas_customer_id
    return create_asaas_customer(client)


def _build_checkout_link(checkout_id: str) -> str:
    public_base = getattr(
        settings,
        "ASAAS_CHECKOUT_PUBLIC_BASE_URL",
        "https://asaas.com/checkoutSession/show?id=",
    )
    if "{id}" in public_base:
        return public_base.format(id=checkout_id)
    return f"{public_base}{checkout_id}"


def create_asaas_billing(client, amount, due_date, recurring_months=1):
    recurring_months = max(int(recurring_months or 1), 1)
    customer_id = ensure_asaas_customer(client)
    subscription_end_date = _add_months(due_date, recurring_months - 1)

    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/checkouts"
    payload = {
        "customer": customer_id,
        "billingTypes": ["CREDIT_CARD"],
        "chargeTypes": ["RECURRENT"],
        "minutesToExpire": getattr(settings, "ASAAS_CHECKOUT_EXPIRATION_MINUTES", 10080),
        "items": [
            {
                "name": f"Mensalidade {client.name}",
                "description": (
                    f"Cobranca recorrente por {recurring_months} mes(es), "
                    f"com inicio em {due_date.strftime('%d/%m/%Y')}"
                ),
                "quantity": 1,
                "value": float(amount),
            }
        ],
        "subscription": {
            "cycle": "MONTHLY",
            "nextDueDate": f"{due_date.strftime('%Y-%m-%d')} 00:00:00",
            "endDate": f"{subscription_end_date.strftime('%Y-%m-%d')} 23:59:59",
        },
    }

    success_url = getattr(settings, "ASAAS_CHECKOUT_SUCCESS_URL", "")
    cancel_url = getattr(settings, "ASAAS_CHECKOUT_CANCEL_URL", "")
    expired_url = getattr(settings, "ASAAS_CHECKOUT_EXPIRED_URL", "")
    if success_url or cancel_url or expired_url:
        payload["callback"] = {
            "successUrl": success_url or cancel_url or expired_url,
            "cancelUrl": cancel_url or success_url or expired_url,
            "expiredUrl": expired_url or cancel_url or success_url,
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
        raise AsaasError(
            f"Erro ao criar checkout recorrente (HTTP {response.status_code}) "
            f"URL={url} payload={payload} detalhe={snippet}"
        )

    data = response.json()
    checkout_id = data.get("id")
    if not checkout_id:
        raise AsaasError("Falha ao criar checkout recorrente no Asaas.")

    return {
        "checkout_id": checkout_id,
        "payment_link": _build_checkout_link(checkout_id),
        "billing_type": "CREDIT_CARD",
        "charge_type": "RECURRENT",
        "subscription_end_date": subscription_end_date,
        "recurring_months": recurring_months,
    }
