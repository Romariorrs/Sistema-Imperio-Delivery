from django.conf import settings
from django.core.management.commands.runserver import Command as RunserverCommand

from run_ngrok import start_ngrok


class Command(RunserverCommand):
    help = "Executa o servidor Django e inicia túneis NGROK automaticamente."

    def inner_run(self, *args, **options):
        if settings.NGROK_ENABLED:
            port = getattr(self, "port", 8000) or 8000
            start_ngrok(port=int(port))
        return super().inner_run(*args, **options)
