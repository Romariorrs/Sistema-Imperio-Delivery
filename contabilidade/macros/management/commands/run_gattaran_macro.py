import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from contabilidade.macros.collector import run_with_metrics
from contabilidade.macros.models import MacroRun

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Executa a macro Selenium para coletar leads e enviar para a API do sistema."

    def add_arguments(self, parser):
        parser.add_argument("--headless", action="store_true", help="Executa navegador em modo headless.")
        parser.add_argument(
            "--manual-login",
            dest="manual_login",
            action="store_true",
            default=True,
            help="Aguarda login/filtro manual na pagina antes da coleta.",
        )
        parser.add_argument(
            "--no-manual-login",
            dest="manual_login",
            action="store_false",
            help="Nao espera login manual.",
        )
        parser.add_argument(
            "--login-timeout",
            type=int,
            default=300,
            help="Tempo maximo (segundos) para esperar tabela apos login/filtro.",
        )
        parser.add_argument("--max-pages", type=int, default=9999, help="Numero maximo de paginas a percorrer.")
        parser.add_argument("--target-url", default="", help="URL alvo da coleta.")
        parser.add_argument("--api-url", default="", help="Endpoint de importacao da API.")
        parser.add_argument("--api-token", default="", help="Token Bearer da API.")
        parser.add_argument("--no-send-api", action="store_true", help="Nao envia para API; apenas coleta.")

    def handle(self, *args, **options):
        api_url = (
            options["api_url"]
            or settings.MACRO_IMPORT_API_URL
            or "http://127.0.0.1:8000/macros/api/import/"
        )
        run_log = MacroRun.objects.create(
            run_type="command",
            status="running",
            source="gattaran",
            message="Macro iniciada.",
        )
        try:
            result = run_with_metrics(
                headless=options["headless"],
                manual_login=options["manual_login"],
                login_timeout=options["login_timeout"],
                max_pages=options["max_pages"],
                send_api=not options["no_send_api"],
                api_url=api_url,
                api_token=options["api_token"] or settings.MACRO_API_TOKEN,
                target_url=options["target_url"] or settings.MACRO_TARGET_URL,
            )
            run_log.status = "success"
            run_log.finished_at = timezone.now()
            run_log.total_collected = result["deduplicated"]
            run_log.total_received = result["deduplicated"]
            run_log.total_sent = result["sent"]
            run_log.message = (
                f"Macro concluida. Coletadas={result['collected']}, "
                f"deduplicadas={result['deduplicated']}, enviadas={result['sent']}."
            )
            run_log.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "total_collected",
                    "total_received",
                    "total_sent",
                    "message",
                ]
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"Macro concluida. Linhas coletadas: {result['collected']} | "
                    f"deduplicadas: {result['deduplicated']} | enviadas: {result['sent']}"
                )
            )
        except Exception as exc:
            run_log.status = "error"
            run_log.finished_at = timezone.now()
            run_log.message = f"Erro na macro: {exc}"
            run_log.save(update_fields=["status", "finished_at", "message"])
            raise
