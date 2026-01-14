import os

import django
from pyngrok import ngrok

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "contabilidade.settings")
django.setup()
from django.conf import settings  # noqa: E402


def start_ngrok(port: int = 8000):
    if not settings.NGROK_ENABLED:
        print("NGROK desabilitado.")
        return None

    if settings.NGROK_AUTHTOKEN:
        ngrok.set_auth_token(settings.NGROK_AUTHTOKEN)

    if settings.NGROK_DOMAIN:
        tunnel = ngrok.connect(addr=port, proto="http", hostname=settings.NGROK_DOMAIN)
    else:
        tunnel = ngrok.connect(addr=port, proto="http")

    public_url = tunnel.public_url
    settings.NGROK_PUBLIC_URL = public_url
    print(f"NGROK ativo: {public_url}")
    return public_url


if __name__ == "__main__":
    start_ngrok()
