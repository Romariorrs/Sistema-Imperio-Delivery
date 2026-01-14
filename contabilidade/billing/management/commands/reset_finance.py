from django.core.management.base import BaseCommand

from contabilidade.billing.models import Billing
from contabilidade.messaging.models import MessageQueue


class Command(BaseCommand):
    help = "Zera os valores de 'Em Caixa' e 'A Receber' limpando cobranças e, opcionalmente, a fila de mensagens."

    def add_arguments(self, parser):
        parser.add_argument(
            "--keep-queue",
            action="store_true",
            help="Não apaga a fila de mensagens (MessageQueue).",
        )

    def handle(self, *args, **options):
        keep_queue = options["keep_queue"]

        bill_count, _ = Billing.objects.all().delete()
        self.stdout.write(self.style.SUCCESS(f"Removidas {bill_count} cobranças."))

        if not keep_queue:
            queue_count, _ = MessageQueue.objects.all().delete()
            self.stdout.write(self.style.SUCCESS(f"Removidos {queue_count} itens da fila de mensagens."))
        else:
            self.stdout.write("Fila de mensagens preservada (--keep-queue).")

        self.stdout.write(self.style.SUCCESS("Financeiro zerado."))
