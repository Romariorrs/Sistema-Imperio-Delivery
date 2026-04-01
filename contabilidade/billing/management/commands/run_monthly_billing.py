import time
from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from contabilidade.billing.models import Billing
from contabilidade.billing.services import AsaasError, create_asaas_billing
from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue, MessageTemplate
from contabilidade.whatsapp.services import WhatsAppClient, WhatsAppError


def _template_by_name(name: str):
    return MessageTemplate.objects.filter(name__iexact=name).first()


def _build_default_message(client, amount, due_date, payment_link, recurring_months, is_reminder=False):
    if is_reminder:
        header = f"Lembrete da assinatura - primeiro vencimento {due_date.strftime('%d/%m/%Y')}"
    else:
        header = f"Assinatura recorrente no cartao por {recurring_months} mes(es)"
    return (
        f"Ola {client.name}!\n"
        f"{header}.\n"
        f"Valor mensal: R$ {amount:.2f}\n"
        f"Link para pagamento: {payment_link}"
    )


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
    help = "Gera checkouts recorrentes quando nao existe assinatura ativa e enfileira mensagens."

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
            help="Janela (dias) para enviar lembretes.",
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
            help="Nao gera novas assinaturas, apenas lembretes.",
        )
        parser.add_argument(
            "--no-remind",
            action="store_true",
            help="Nao gera lembretes, apenas novas assinaturas.",
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

        if create_new:
            clients = Client.objects.filter(active=True)
            for client in clients:
                if not client.default_amount or client.default_amount <= 0:
                    skipped += 1
                    continue
                if not client.phone:
                    skipped += 1
                    continue
                active_plan = (
                    Billing.objects.filter(
                        client=client,
                        charge_type="RECURRENT",
                        status__in=["pending", "paid", "overdue"],
                        subscription_end_date__gte=today,
                    )
                    .exclude(status="canceled")
                    .exists()
                )
                if active_plan:
                    skipped += 1
                    continue

                due_date = today
                last_billing = Billing.objects.filter(client=client).order_by("-subscription_end_date", "-due_date").first()
                if not last_billing:
                    due_date = client.created_at.date() + timedelta(days=days_ahead)
                    if due_date < today:
                        due_date = today

                try:
                    checkout = create_asaas_billing(
                        client,
                        client.default_amount,
                        due_date,
                        recurring_months=client.recurring_months,
                    )
                    billing = Billing.objects.create(
                        client=client,
                        amount=client.default_amount,
                        due_date=due_date,
                        subscription_end_date=checkout["subscription_end_date"],
                        recurring_months=checkout["recurring_months"],
                        status="pending",
                        billing_type=checkout["billing_type"],
                        charge_type=checkout["charge_type"],
                        asaas_checkout_id=checkout["checkout_id"],
                        payment_link=checkout["payment_link"] or "",
                    )
                except AsaasError as exc:
                    errors += 1
                    self.stdout.write(self.style.ERROR(f"{client.name}: {exc}"))
                    continue

                if tpl_new:
                    final_text = _build_message(
                        tpl_new,
                        client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                        billing.recurring_months,
                    )
                else:
                    final_text = _build_default_message(
                        client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                        billing.recurring_months,
                    )
                MessageQueue.objects.create(
                    client=client,
                    billing=billing,
                    template=tpl_new,
                    final_text=final_text,
                )
                created += 1
                queued += 1

        if remind:
            recent_cutoff = timezone.now() - timedelta(days=cooldown_days)
            pendings = Billing.objects.filter(status__in=["pending", "overdue"], charge_type="RECURRENT")
            for billing in pendings:
                if not billing.client.phone:
                    continue
                if MessageQueue.objects.filter(billing=billing, created_at__gte=recent_cutoff).exists():
                    continue

                if tpl_reminder:
                    final_text = _build_message(
                        tpl_reminder,
                        billing.client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                        billing.recurring_months,
                    )
                else:
                    final_text = _build_default_message(
                        billing.client,
                        billing.amount,
                        billing.due_date,
                        billing.payment_link,
                        billing.recurring_months,
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
