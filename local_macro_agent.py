import argparse
import html
import json
import os
import threading
import time
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from contabilidade.macros.collector import run_with_metrics


STATE_LOCK = threading.Lock()
STATE = {
    "running": False,
    "last_status": "idle",
    "started_at": "",
    "finished_at": "",
    "last_result": {},
    "last_error": "",
}

DEFAULT_TARGET_URL = "https://gattaran.didi-food.com/v2/gtr_crm/leads/list/all"
DEFAULT_PROFILE_DIR = os.path.join(
    os.getenv("LOCALAPPDATA") or os.getcwd(),
    "ImperioMacro",
    "chrome_user_data",
)


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _snapshot_state():
    with STATE_LOCK:
        return dict(STATE)


def _update_state(**kwargs):
    with STATE_LOCK:
        STATE.update(kwargs)


def _run_collection_job(config):
    _update_state(
        running=True,
        last_status="running",
        started_at=_now_text(),
        finished_at="",
        last_result={},
        last_error="",
    )
    try:
        profile_dir = (config.get("profile_dir") or "").strip() or DEFAULT_PROFILE_DIR
        os.makedirs(profile_dir, exist_ok=True)
        os.environ["USE_CHROME_PROFILE"] = "1"
        os.environ["CHROME_USER_DATA_DIR"] = profile_dir
        os.environ["CHROME_PROFILE_DIR"] = "Default"

        result = run_with_metrics(
            headless=_parse_bool(config.get("headless"), False),
            manual_login=_parse_bool(config.get("manual_login"), True),
            login_timeout=_parse_int(config.get("login_timeout"), 900),
            max_pages=_parse_int(config.get("max_pages"), 9999),
            send_api=True,
            api_url=(config.get("api_url") or "").strip(),
            api_token=(config.get("api_token") or "").strip(),
            target_url=(config.get("target_url") or "").strip() or DEFAULT_TARGET_URL,
        )
        _update_state(
            running=False,
            last_status="success",
            finished_at=_now_text(),
            last_result=result,
            last_error="",
        )
    except Exception:
        _update_state(
            running=False,
            last_status="error",
            finished_at=_now_text(),
            last_result={},
            last_error=traceback.format_exc(limit=8),
        )


def _start_job(config):
    snap = _snapshot_state()
    if snap["running"]:
        return False, "Coleta ja esta em execucao."
    if not (config.get("api_url") or "").strip():
        return False, "Informe API URL."
    if not (config.get("api_token") or "").strip():
        return False, "Informe API Token."

    thread = threading.Thread(target=_run_collection_job, args=(config,), daemon=True)
    thread.start()
    return True, "Coleta iniciada."


def _html_page(params):
    api_url = html.escape((params.get("api_url", [""])[0]).strip())
    api_token = html.escape((params.get("api_token", [""])[0]).strip())
    target_url = html.escape((params.get("target_url", [""])[0]).strip())
    if not target_url:
        target_url = DEFAULT_TARGET_URL
    profile_dir = html.escape((params.get("profile_dir", [""])[0]).strip() or DEFAULT_PROFILE_DIR)
    manual_login_checked = "checked"
    return f"""<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Coletor Local - Imperio</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 18px; background: #0f1725; color: #e8eefc; }}
    .card {{ background: #131f32; border: 1px solid #243655; border-radius: 12px; padding: 14px; margin-bottom: 12px; }}
    label {{ display:block; margin: 8px 0 4px; color: #9fb2d7; }}
    input {{ width: 100%; padding: 10px; border: 1px solid #2a3d60; border-radius: 8px; background: #0f1725; color: #e8eefc; }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .buttons {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    button, a.btn {{ border: none; background: #34d1af; color: #08101c; padding: 10px 12px; border-radius: 8px; font-weight: 600; text-decoration: none; cursor: pointer; }}
    a.secondary, button.secondary {{ background: #203251; color: #e8eefc; }}
    pre {{ white-space: pre-wrap; background: #0a1220; border: 1px solid #253957; border-radius: 8px; padding: 10px; }}
  </style>
</head>
<body>
  <h2>Coletor Local (sem PowerShell)</h2>
  <div class="card">
    <p>1) Clique em "Abrir pagina alvo".</p>
    <p>2) Faca login e aplique o filtro desejado.</p>
    <p>3) Volte aqui e clique em "Comecar coleta".</p>
    <p style="color:#9fb2d7;margin-top:10px;">Depois do primeiro login, a sessao fica salva neste computador.</p>
  </div>
  <div class="card">
    <form method="post" action="/start">
      <label>API URL (do seu sistema)</label>
      <input name="api_url" value="{api_url}" required>
      <label>API Token</label>
      <input name="api_token" value="{api_token}" required>
      <label>URL alvo</label>
      <input name="target_url" value="{target_url}" required>
      <label>Pasta do perfil (mantem login)</label>
      <input name="profile_dir" value="{profile_dir}" required>
      <div class="row">
        <div>
          <label>Login timeout (segundos)</label>
          <input name="login_timeout" value="900">
        </div>
        <div>
          <label>Max paginas</label>
          <input name="max_pages" value="9999">
        </div>
      </div>
      <label><input type="checkbox" name="manual_login" value="1" {manual_login_checked} style="width:auto;"> Esperar login/filtro manual</label>
      <div class="buttons">
        <a class="btn secondary" href="{target_url}" target="_blank" rel="noopener">Abrir pagina alvo</a>
        <button type="submit">Comecar coleta</button>
      </div>
    </form>
  </div>
  <div class="card">
    <h3>Status</h3>
    <pre id="statusBox">Carregando...</pre>
  </div>
  <script>
    function updateStatus() {{
      fetch('/status')
        .then(r => r.json())
        .then(data => {{
          document.getElementById('statusBox').textContent = JSON.stringify(data, null, 2);
        }})
        .catch(err => {{
          document.getElementById('statusBox').textContent = 'Erro ao ler status: ' + err;
        }});
    }}
    updateStatus();
    setInterval(updateStatus, 2000);
  </script>
</body>
</html>"""


class MacroAgentHandler(BaseHTTPRequestHandler):
    def _send_html(self, content, status=HTTPStatus.OK):
        body = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body_data(self):
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        ctype = (self.headers.get("Content-Type") or "").lower()
        if "application/json" in ctype:
            try:
                data = json.loads(raw.decode("utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                return {}
            return {}
        params = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        return {key: values[0] if values else "" for key, values in params.items()}

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/":
            return self._send_html(_html_page(params))
        if parsed.path == "/status":
            return self._send_json(_snapshot_state())
        if parsed.path == "/start":
            data = {key: values[0] if values else "" for key, values in params.items()}
            started, message = _start_job(data)
            status = HTTPStatus.OK if started else HTTPStatus.CONFLICT
            return self._send_json({"ok": started, "message": message}, status=status)
        return self._send_json({"ok": False, "detail": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/start":
            data = self._read_body_data()
            started, message = _start_job(data)
            status = HTTPStatus.OK if started else HTTPStatus.CONFLICT
            return self._send_json({"ok": started, "message": message}, status=status)
        return self._send_json({"ok": False, "detail": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, fmt, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Agente local para coleta da macro no Windows.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), MacroAgentHandler)
    print(f"[macro-agent] Rodando em http://{args.host}:{args.port}/")
    print("[macro-agent] Abra essa URL no navegador e use os botoes.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
