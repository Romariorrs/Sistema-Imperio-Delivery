import datetime
import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client


STATUS_MAP = {
    "pending": "pending",
    "overdue": "overdue",
    "received": "paid",
    "received_in_cash": "paid",
    "confirmed": "paid",
    "canceled": "canceled",
    "refunded": "canceled",
}


class Command(BaseCommand):
    help = "Sincroniza cobranças do Asaas com o banco local (status, valor, vencimento, link)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Número de registros por página (padrão: 100)",
        )

    def handle(self, *args, **options):
        api_key = settings.ASAAS_API_KEY
        if not api_key:
            raise CommandError("ASAAS_API_KEY não configurada no ambiente/.env")

        base_url = settings.ASAAS_API_BASE_URL.rstrip("/")
        url = f"{base_url}/payments"
        headers = {"access_token": api_key}
        limit = options["limit"]
        offset = 0

        updated = 0
        not_found = 0
        created = 0
        total = 0

        while True:
            params = {"offset": offset, "limit": limit}
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if not resp.ok:
                raise CommandError(f"Erro ao buscar payments: {resp.status_code} {resp.text}")

            data = resp.json()
            items = data.get("data") or data.get("content") or []
            if not items:
                break

            for pay in items:
                total += 1
                pid = pay.get("id")
                if not pid:
                    continue
                status_raw = (pay.get("status") or "").lower()
                new_status = STATUS_MAP.get(status_raw)
                try:
                    billing = Billing.objects.get(asaas_billing_id=pid)
                except Billing.DoesNotExist:
                    # tenta criar se encontrar cliente pelo customer do Asaas
                    customer_id = pay.get("customer")
                    client = None
                    if customer_id:
                        client = Client.objects.filter(asaas_customer_id=customer_id).first()
                    if client:
                        due = None
                        if pay.get("dueDate"):
                            try:
                                due = datetime.datetime.strptime(pay["dueDate"], "%Y-%m-%d").date()
                            except Exception:
                                pass
                        link = pay.get("invoiceUrl") or pay.get("bankSlipUrl") or pay.get("paymentLink")
                        billing = Billing.objects.create(
                            client=client,
                            amount=pay.get("value") or 0,
                            due_date=due or datetime.date.today(),
                            status=STATUS_MAP.get(status_raw, "pending"),
                            asaas_billing_id=pid,
                            payment_link=link or "",
                        )
                        created += 1
                    else:
                        not_found += 1
                        continue

                update_fields = []
                if new_status and billing.status != new_status:
                    billing.status = new_status
                    update_fields.append("status")

                if pay.get("value") is not None and billing.amount != pay["value"]:
                    billing.amount = pay["value"]
                    update_fields.append("amount")

                if pay.get("dueDate"):
                    try:
                        due = datetime.datetime.strptime(pay["dueDate"], "%Y-%m-%d").date()
                        if billing.due_date != due:
                            billing.due_date = due
                            update_fields.append("due_date")
                    except Exception:
                        pass

                link = pay.get("invoiceUrl") or pay.get("bankSlipUrl") or pay.get("paymentLink")
                if link and billing.payment_link != link:
                    billing.payment_link = link
                    update_fields.append("payment_link")

                if update_fields:
                    billing.save(update_fields=list(set(update_fields)))
                    updated += 1

            offset += len(items)
            if len(items) < limit:
                break

        self.stdout.write(
            self.style.SUCCESS(
                f"Sync finalizado. Lidos: {total}, atualizados: {updated}, criados: {created}, não encontrados: {not_found}"
            )
        )
