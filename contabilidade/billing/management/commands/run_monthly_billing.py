import calendar
import time
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from contabilidade.billing.models import Billing
from contabilidade.billing.services import AsaasError, create_asaas_billing
from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue, MessageTemplate
from contabilidade.whatsapp.services import WhatsAppClient, WhatsAppError


def _add_one_month(value: date) -> date:
    year = value.year + (value.month // 12)
    month = 1 if value.month == 12 else value.month + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(value.day, last_day)
    return date(year, month, day)


def _template_by_name(name: str):
    return MessageTemplate.objects.filter(name__iexact=name).first()


def _build_default_message(client, amount, due_date, payment_link, is_reminder: bool = False):
    if is_reminder:
        header = f"Lembrete de pagamento - vencimento {due_date.strftime('%d/%m/%Y')}"
    else:
        header = f"Mensalidade do mes {due_date.strftime('%m/%Y')}"
    return (
        f"Ola {client.name}!\n"
        f"{header}.\n"
        f"Valor: R$ {amount:.2f}\n"
        f"Link para pagamento: {payment_link}"
    )


def _build_message(template, client, amount, due_date, payment_link):
    text = template.body
    text = text.replace("{nome}", client.name)
    text = text.replace("{valor}", f"{amount:.2f}")
    text = text.replace("{vencimento}", due_date.strftime("%d/%m/%Y"))
    text = text.replace("{link}", payment_link or "")
    if payment_link and "{link}" not in template.body:
        text = f"{text}\nLink de pagamento: {payment_link}"
    return text


def _send_queue(delay: float):
    client = WhatsAppClient()
    pending = MessageQueue.objects.filter(status__in=["pending", "error"]).order_by("created_at")
    for msg in pending:
        try:
            client.send_message(msg.client.phone, msg.final_text)
            msg.status = "sent"
            msg.sent_at = timezone.now()
            msg.error_message = ""
        except WhatsAppError as exc:
            msg.status = "error"
            msg.error_message = str(exc)
            msg.sent_at = timezone.now()
        msg.attempts += 1
        msg.save()
        time.sleep(delay)


class Command(BaseCommand):
    help = "Gera cobrancas mensais e enfileira mensagens com opcao de envio imediato."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days-ahead",
            type=int,
            default=30,
            help="Dias apos cadastro para o primeiro vencimento.",
        )
        parser.add_argument(
            "--remind-days",
            type=int,
            default=2,
            help="Janela (dias) para enviar lembretes (use 0 para apenas atrasados).",
        )
        parser.add_argument(
            "--cooldown-days",
            type=int,
            default=2,
            help="Evita lembretes repetidos dentro deste periodo.",
        )
        parser.add_argument(
            "--no-create",
            action="store_true",
            help="Nao gera novas cobrancas, apenas lembretes.",
        )
        parser.add_argument(
            "--no-remind",
            action="store_true",
            help="Nao gera lembretes, apenas cobrancas novas.",
        )
        parser.add_argument(
            "--send-now",
            action="store_true",
            help="Envia a fila imediatamente via WhatsApp.",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=4.0,
            help="Intervalo entre envios (segundos) quando --send-now.",
        )

    def handle(self, *args, **options):
        today = date.today()
        days_ahead = options["days_ahead"]
        remind_days = options["remind_days"]
        cooldown_days = options["cooldown_days"]
        create_new = not options["no_create"]
        remind = not options["no_remind"]

        created = 0
        queued = 0
        reminder_queued = 0
        skipped = 0
        errors = 0

        tpl_new = _template_by_name("Mensalidade")
        tpl_reminder = _template_by_name("Lembrete")

        queued_clients = set()
        if create_new:
            clients = Client.objects.filter(active=True)
            for client in clients:
                if not client.default_amount or client.default_amount <= 0:
                    skipped += 1
                    continue
                if not client.phone:
                    skipped += 1
                    continue

                last_billing = Billing.objects.filter(client=client).order_by("-due_date").first()
                if last_billing:
                    due_date = _add_one_month(last_billing.due_date)
                else:
                    due_date = client.created_at.date() + timedelta(days=days_ahead)

                if due_date > today:
                    skipped += 1
                    continue
                if due_date < today:
                    due_date = today

                if Billing.objects.filter(client=client, due_date=due_date).exclude(status="canceled").exists():
                    skipped += 1
                    continue

                try:
                    billing_id, payment_link = create_asaas_billing(
                        client, client.default_amount, due_date
                    )
                    billing = Billing.objects.create(
                        client=client,
                        amount=client.default_amount,
                        due_date=due_date,
                        status="pending",
                        asaas_billing_id=billing_id,
                        payment_link=payment_link or "",
                    )
                except AsaasError as exc:
                    errors += 1
                    self.stdout.write(self.style.ERROR(f"{client.name}: {exc}"))
                    continue

                if tpl_new:
                    final_text = _build_message(
                        tpl_new, client, billing.amount, billing.due_date, billing.payment_link
                    )
                else:
                    final_text = _build_default_message(
                        client, billing.amount, billing.due_date, billing.payment_link
                    )
                MessageQueue.objects.create(
                    client=client,
                    billing=billing,
                    template=tpl_new,
                    final_text=final_text,
                )
                queued_clients.add(client.id)
                created += 1
                queued += 1

        if remind:
            remind_until = today + timedelta(days=remind_days)
            recent_cutoff = timezone.now() - timedelta(days=cooldown_days)
            pendings = Billing.objects.filter(status__in=["pending", "overdue"])
            for billing in pendings:
                if billing.status == "pending" and billing.due_date < today:
                    billing.status = "overdue"
                    billing.save(update_fields=["status"])
                if billing.due_date > remind_until:
                    continue
                if billing.due_date >= today:
                    continue
                if not billing.client.phone:
                    continue
                if billing.client_id in queued_clients:
                    continue
                if MessageQueue.objects.filter(
                    billing=billing, created_at__gte=recent_cutoff
                ).exists():
                    continue

                if tpl_reminder:
                    final_text = _build_message(
                        tpl_reminder,
                        billing.client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                    )
                else:
                    final_text = _build_default_message(
                        billing.client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                        is_reminder=True,
                    )
                MessageQueue.objects.create(
                    client=billing.client,
                    billing=billing,
                    template=tpl_reminder,
                    final_text=final_text,
                )
                reminder_queued += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Novas cobrancas: {created}, mensagens enfileiradas: {queued}, "
                f"lembretes enfileirados: {reminder_queued}, ignorados: {skipped}, erros: {errors}"
            )
        )

        if options["send_now"]:
            self.stdout.write(self.style.WARNING("Enviando fila de mensagens..."))
            try:
                _send_queue(options["delay"])
                self.stdout.write(self.style.SUCCESS("Envio finalizado."))
            except WhatsAppError as exc:
                self.stdout.write(self.style.ERROR(f"Erro ao enviar via WhatsApp: {exc}"))
