from typing import Optional

from .services import WhatsAppClient

_client: Optional[WhatsAppClient] = None


def reset_client():
    global _client
    _client = None


def get_client(headless: bool = True):  # headless mantido para compatibilidade
    global _client
    if _client is None:
        _client = WhatsAppClient()
    return _client
