import time

from django.core.management.base import BaseCommand
from django.utils import timezone

from contabilidade.messaging.models import MessageQueue
from contabilidade.whatsapp.services import WhatsAppClient, WhatsAppError


class Command(BaseCommand):
    help = "Processa a fila de mensagens pendentes via WhatsApp Web (Selenium)."

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=2.0, help="Intervalo entre envios (segundos)")
        parser.add_argument("--headless", action="store_true", help="Executa navegador em modo headless")

    def handle(self, *args, **options):
        delay = options["delay"]
        client = WhatsAppClient(headless=options["headless"])
        pending = MessageQueue.objects.filter(status="pending").order_by("created_at")
        for msg in pending:
            try:
                client.send_message(msg.client.phone, msg.final_text)
                msg.status = "sent"
                msg.sent_at = timezone.now()
            except WhatsAppError as exc:
                msg.status = "error"
                msg.error_message = str(exc)
            msg.attempts += 1
            msg.save()
            time.sleep(delay)
        self.stdout.write(self.style.SUCCESS("Processamento finalizado."))
