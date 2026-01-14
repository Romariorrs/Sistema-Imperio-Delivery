import os
import re
from urllib.parse import quote

import requests


class WhatsAppError(Exception):
    pass


class WhatsAppClient:
    """
    Cliente baseado na Evolution API (HTTP).
    Configure no .env:
      EVOLUTION_API_BASE_URL=https://sua-url
      EVOLUTION_API_TOKEN=SEU_TOKEN
      EVOLUTION_API_INSTANCE=ID_DA_INSTANCIA
    """

    def __init__(self):
        self.base_url = os.getenv("EVOLUTION_API_BASE_URL", "").rstrip("/")
        self.token = os.getenv("EVOLUTION_API_TOKEN")
        self.instance = os.getenv("EVOLUTION_API_INSTANCE")
        if not self.base_url or not self.token or not self.instance:
            raise WhatsAppError(
                "Configurações da Evolution API ausentes. "
                "Defina EVOLUTION_API_BASE_URL, EVOLUTION_API_TOKEN e EVOLUTION_API_INSTANCE no .env."
            )

    def _headers(self):
        # Algumas instâncias da Evolution usam Authorization, outras usam apikey.
        return {
            "Authorization": f"Bearer {self.token}",
            "apikey": self.token,
            "Content-Type": "application/json",
        }

    def get_connection_status(self):
        url = f"{self.base_url}/instance/connectionState/{self.instance}"
        try:
            res = requests.get(url, headers=self._headers(), timeout=10)
            if res.ok:
                data = res.json()
                state = (
                    data.get("state")
                    or data.get("connectionStatus")
                    or data.get("result")
                    or ""
                )
                state = str(state).lower()
                if "qr" in state:
                    return "qr"
                if state in {"connected", "open", "online", "ready"}:
                    return "connected"
                return state or "loading"
            return "error"
        except Exception:
            return "error"

    def get_qr_screenshot_base64(self):
        """
        Tenta obter o QR code via API, caso esteja disponível.
        Retorna base64 ou None.
        """
        url = f"{self.base_url}/instance/qr/{self.instance}"
        try:
            res = requests.get(url, headers=self._headers(), timeout=10)
            if res.ok:
                data = res.json()
                return data.get("qr") or data.get("base64") or data.get("image")
        except Exception:
            pass
        return None

    def send_message(self, phone: str, text: str):
        if not phone:
            raise WhatsAppError("Telefone não informado.")
        digits = re.sub(r"\D", "", phone)
        if len(digits) < 10:
            raise WhatsAppError(f"Telefone inválido: {phone}")
        if not digits.startswith("55"):
            digits = "55" + digits

        url = f"{self.base_url}/message/sendText/{self.instance}"
        payload = {"number": digits, "text": text}
        res = requests.post(url, headers=self._headers(), json=payload, timeout=15)
        if not res.ok:
            try:
                detail = res.json()
            except Exception:
                detail = res.text
            raise WhatsAppError(f"Erro ao enviar mensagem: {detail}")
