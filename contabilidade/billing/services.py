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


def _default_callback_base_url() -> str:
    candidates = [
        getattr(settings, "ASAAS_CHECKOUT_CALLBACK_BASE_URL", ""),
        *getattr(settings, "CSRF_TRUSTED_ORIGINS", []),
    ]
    for host in getattr(settings, "ALLOWED_HOSTS", []):
        host = (host or "").strip()
        if not host or host in {"*", "localhost", "127.0.0.1"}:
            continue
        candidates.append(f"https://{host}")

    for candidate in candidates:
        candidate = (candidate or "").strip().rstrip("/")
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
    return ""


def _build_callback_urls():
    base_url = _default_callback_base_url()
    success_url = getattr(settings, "ASAAS_CHECKOUT_SUCCESS_URL", "").strip()
    cancel_url = getattr(settings, "ASAAS_CHECKOUT_CANCEL_URL", "").strip()
    expired_url = getattr(settings, "ASAAS_CHECKOUT_EXPIRED_URL", "").strip()

    if not success_url and base_url:
        success_url = f"{base_url}/billing/mensalidades/"
    if not cancel_url and base_url:
        cancel_url = f"{base_url}/billing/mensalidades/"
    if not expired_url and base_url:
        expired_url = f"{base_url}/billing/mensalidades/"

    if not (success_url and cancel_url and expired_url):
        raise AsaasError(
            "Configure um callback do checkout no Asaas. Defina ASAAS_CHECKOUT_CALLBACK_BASE_URL "
            "ou as URLs ASAAS_CHECKOUT_SUCCESS_URL, ASAAS_CHECKOUT_CANCEL_URL e "
            "ASAAS_CHECKOUT_EXPIRED_URL."
        )

    return {
        "successUrl": success_url,
        "cancelUrl": cancel_url,
        "expiredUrl": expired_url,
    }


def create_asaas_billing(client, amount, due_date, recurring_months=1):
    recurring_months = max(int(recurring_months or 1), 1)
    amount_value = float(amount)
    if amount_value < 5:
        raise AsaasError("O valor minimo para checkout recorrente no Asaas e R$ 5,00.")

    customer_id = ensure_asaas_customer(client)
    subscription_end_date = _add_months(due_date, recurring_months - 1)
    item_name = f"Mensalidade {client.name}"[:30]

    url = f"{settings.ASAAS_API_BASE_URL.rstrip('/')}/checkouts"
    payload = {
        "customer": customer_id,
        "billingTypes": ["CREDIT_CARD"],
        "chargeTypes": ["RECURRENT"],
        "minutesToExpire": getattr(settings, "ASAAS_CHECKOUT_EXPIRATION_MINUTES", 100),
        "items": [
            {
                "name": item_name,
                "description": (
                    f"Cobranca recorrente por {recurring_months} mes(es), "
                    f"com inicio em {due_date.strftime('%d/%m/%Y')}"
                ),
                "quantity": 1,
                "value": amount_value,
            }
        ],
        "subscription": {
            "cycle": "MONTHLY",
            "nextDueDate": f"{due_date.strftime('%Y-%m-%d')} 00:00:00",
            "endDate": f"{subscription_end_date.strftime('%Y-%m-%d')} 23:59:59",
        },
        "callback": _build_callback_urls(),
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
        "payment_link": data.get("link") or _build_checkout_link(checkout_id),
        "billing_type": "CREDIT_CARD",
        "charge_type": "RECURRENT",
        "subscription_end_date": subscription_end_date,
        "recurring_months": recurring_months,
    }
