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

from contabilidade.macros.collector import build_driver, run_with_metrics

VERSION = os.getenv("MACRO_AGENT_VERSION", "2026.02.10-4").strip()


STATE_LOCK = threading.Lock()
STATE = {
    "running": False,
    "last_status": "idle",
    "started_at": "",
    "finished_at": "",
    "last_result": {},
    "last_error": "",
}
BROWSER_LOCK = threading.Lock()
BROWSER_DRIVER = None

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


def _apply_profile_env(config):
    profile_dir = (config.get("profile_dir") or "").strip() or DEFAULT_PROFILE_DIR
    os.makedirs(profile_dir, exist_ok=True)
    os.environ["USE_CHROME_PROFILE"] = "1"
    os.environ["CHROME_USER_DATA_DIR"] = profile_dir
    os.environ["CHROME_PROFILE_DIR"] = "Default"
    return profile_dir


def _get_browser_driver():
    with BROWSER_LOCK:
        return BROWSER_DRIVER


def _set_browser_driver(driver):
    global BROWSER_DRIVER
    with BROWSER_LOCK:
        BROWSER_DRIVER = driver


def _ensure_browser_session(config, open_target: bool = True):
    profile_dir = _apply_profile_env(config)
    target_url = (config.get("target_url") or "").strip() or DEFAULT_TARGET_URL
    driver = _get_browser_driver()

    if driver is not None:
        try:
            _ = driver.current_url
        except Exception:
            driver = None
            _set_browser_driver(None)

    if driver is None:
        driver = build_driver(headless=False)
        _set_browser_driver(driver)
        open_target = True

    if open_target:
        driver.get(target_url)

    return driver, profile_dir, target_url


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
        existing = _get_browser_driver()
        if existing is not None:
            try:
                _ = existing.current_url
            except Exception:
                existing = None
                _set_browser_driver(None)
        if existing is None:
            raise RuntimeError("Sessao do navegador nao encontrada. Clique em 'Abrir pagina alvo' primeiro.")

        result = run_with_metrics(
            headless=_parse_bool(config.get("headless"), False),
            manual_login=_parse_bool(config.get("manual_login"), True),
            login_timeout=_parse_int(config.get("login_timeout"), 900),
            max_pages=_parse_int(config.get("max_pages"), 9999),
            send_api=True,
            api_url=(config.get("api_url") or "").strip(),
            api_token=(config.get("api_token") or "").strip(),
            target_url=(config.get("target_url") or "").strip() or DEFAULT_TARGET_URL,
            existing_driver=existing,
            navigate_to_target=False,
            close_driver=False,
        )
        sent = int(result.get("sent") or 0)
        to_send = int(result.get("to_send") or 0)
        if to_send > 0 and sent < to_send:
            _update_state(
                running=False,
                last_status="error",
                finished_at=_now_text(),
                last_result=result,
                last_error=(
                    f"Coleta concluida, mas envio parcial/falhou: enviados {sent} de {to_send}. "
                    "Verifique API URL/token e conexao."
                ),
            )
        else:
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
    existing = _get_browser_driver()
    if existing is None:
        return False, "Clique em 'Abrir pagina alvo' primeiro."
    try:
        _ = existing.current_url
    except Exception:
        _set_browser_driver(None)
        return False, "Sessao do navegador foi fechada. Clique em 'Abrir pagina alvo' novamente."

    thread = threading.Thread(target=_run_collection_job, args=(config,), daemon=True)
    thread.start()
    return True, "Coleta iniciada."


def _prepare_job(config):
    try:
        _, profile_dir, target_url = _ensure_browser_session(config, open_target=True)
        return True, f"Navegador preparado em {target_url} com perfil {profile_dir}"
    except Exception:
        return False, traceback.format_exc(limit=6)


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
    :root {{
      --bg: #070707;
      --surface: rgba(18,18,18,.82);
      --surface-soft: rgba(255,255,255,.03);
      --border: rgba(255,255,255,.14);
      --text: #f4f4f4;
      --muted: #adadad;
      --orange: #f97316;
      --orange-2: #ff9a3d;
      --radius: 14px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", Arial, sans-serif;
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(circle at 65% 10%, rgba(249,115,22,.17), transparent 36%),
        linear-gradient(180deg, #080808, #050505);
      position: relative;
      padding: 18px;
    }}
    body::before {{
      content: "";
      position: fixed;
      inset: 0;
      background-image: repeating-linear-gradient(
        90deg,
        rgba(255,255,255,.06) 0,
        rgba(255,255,255,.06) 1px,
        transparent 1px,
        transparent calc(20% - 1px)
      );
      opacity: .22;
      pointer-events: none;
    }}
    .wrap {{
      position: relative;
      z-index: 1;
      max-width: 1240px;
      margin: 0 auto;
    }}
    .head {{
      margin-bottom: 12px;
    }}
    .head h2 {{
      margin: 0;
      font-size: 36px;
      line-height: 1;
      letter-spacing: -.02em;
    }}
    .version {{
      margin-top: 8px;
      color: #ffd3ab;
      letter-spacing: .14em;
      text-transform: uppercase;
      font-size: 12px;
      font-weight: 700;
    }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 14px;
      margin-bottom: 12px;
      backdrop-filter: blur(8px);
      box-shadow: 0 20px 44px rgba(0,0,0,.34);
    }}
    .card h3 {{
      margin: 0 0 10px;
      font-size: 22px;
      line-height: 1;
    }}
    .steps p {{ margin: 12px 0; color: #dbdbdb; font-size: 28px; line-height: 1.35; }}
    label {{
      display: block;
      margin: 8px 0 4px;
      color: #e0e0e0;
      font-size: 12px;
      letter-spacing: .07em;
      text-transform: uppercase;
      font-weight: 700;
    }}
    input {{
      width: 100%;
      padding: 10px 11px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: rgba(11,11,11,.72);
      color: #fff;
    }}
    input:focus {{
      outline: none;
      border-color: rgba(249,115,22,.75);
      box-shadow: 0 0 0 3px rgba(249,115,22,.22);
    }}
    .row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .checkbox {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      margin-top: 8px;
    }}
    .checkbox input {{
      width: auto;
      margin: 0;
    }}
    .buttons {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }}
    .btn {{
      --btn-surface: linear-gradient(180deg, #ff932f, #f97316);
      position: relative;
      border: 1px solid transparent;
      border-radius: 999px;
      padding: 10px 16px;
      text-decoration: none;
      cursor: pointer;
      font-weight: 700;
      letter-spacing: .06em;
      text-transform: uppercase;
      color: #fff;
      background: transparent;
      isolation: isolate;
      overflow: hidden;
      min-height: 40px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .btn::before {{
      content: "";
      position: absolute;
      inset: -2px;
      border-radius: inherit;
      background: conic-gradient(
        from 0deg,
        rgba(255,132,36,0) 0deg,
        rgba(255,132,36,0) 305deg,
        rgba(255,132,36,.95) 330deg,
        rgba(255,132,36,0) 360deg
      );
      animation: orbit 2.7s linear infinite;
      z-index: -2;
    }}
    .btn::after {{
      content: "";
      position: absolute;
      inset: 1px;
      border-radius: inherit;
      background: var(--btn-surface);
      z-index: -1;
    }}
    .btn.secondary {{
      --btn-surface: rgba(26,26,26,.94);
      color: #ececec;
    }}
    @keyframes orbit {{
      to {{ transform: rotate(1turn); }}
    }}
    pre {{
      white-space: pre-wrap;
      background: rgba(8,8,8,.72);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      color: #e8e8e8;
      margin: 0;
    }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 900px) {{
      .row {{ grid-template-columns: 1fr; }}
      .head h2 {{ font-size: 28px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <h2>Coletor Local (sem PowerShell)</h2>
      <div class="version">Versao: {VERSION}</div>
    </div>
    <div class="card steps">
      <p>1) Clique em "Abrir pagina alvo".</p>
      <p>2) Faca login e aplique o filtro desejado.</p>
      <p>3) Volte aqui e clique em "Comecar coleta".</p>
      <p class="muted">Depois do primeiro login, a sessao fica salva neste computador.</p>
    </div>
    <div class="card">
      <form id="startForm">
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
        <label class="checkbox"><input type="checkbox" name="manual_login" value="1" {manual_login_checked}> Esperar login/filtro manual</label>
        <div class="buttons">
          <button type="button" class="btn secondary" id="openBtn">Abrir pagina alvo</button>
          <button type="button" class="btn" id="startBtn">Comecar coleta</button>
        </div>
      </form>
    </div>
    <div class="card">
      <h3>Mensagens</h3>
      <pre id="msgBox">Pronto para iniciar.</pre>
    </div>
    <div class="card">
      <h3>Status</h3>
      <pre id="statusBox">Carregando...</pre>
    </div>
  </div>
  <script>
    const form = document.getElementById('startForm');
    const msgBox = document.getElementById('msgBox');
    const openBtn = document.getElementById('openBtn');
    const startBtn = document.getElementById('startBtn');

    openBtn.addEventListener('click', async () => {{
      const data = new URLSearchParams(new FormData(form));
      msgBox.textContent = 'Abrindo pagina alvo no navegador do coletor...';
      try {{
        const resp = await fetch('/prepare', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/x-www-form-urlencoded' }},
          body: data.toString()
        }});
        const payload = await resp.json();
        msgBox.textContent = JSON.stringify(payload, null, 2);
      }} catch (err) {{
        msgBox.textContent = 'Erro ao abrir pagina alvo: ' + err;
      }}
      updateStatus();
    }});

    startBtn.addEventListener('click', async () => {{
      const data = new URLSearchParams(new FormData(form));
      msgBox.textContent = 'Iniciando coleta...';
      try {{
        const resp = await fetch('/start', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/x-www-form-urlencoded' }},
          body: data.toString()
        }});
        const payload = await resp.json();
        msgBox.textContent = JSON.stringify(payload, null, 2);
      }} catch (err) {{
        msgBox.textContent = 'Erro ao iniciar coleta: ' + err;
      }}
      updateStatus();
    }});

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
        if parsed.path == "/prepare":
            data = {key: values[0] if values else "" for key, values in params.items()}
            ok, message = _prepare_job(data)
            status = HTTPStatus.OK if ok else HTTPStatus.INTERNAL_SERVER_ERROR
            return self._send_json({"ok": ok, "message": message}, status=status)
        if parsed.path == "/start":
            data = {key: values[0] if values else "" for key, values in params.items()}
            started, message = _start_job(data)
            status = HTTPStatus.OK if started else HTTPStatus.CONFLICT
            return self._send_json({"ok": started, "message": message}, status=status)
        return self._send_json({"ok": False, "detail": "not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/prepare":
            data = self._read_body_data()
            ok, message = _prepare_job(data)
            status = HTTPStatus.OK if ok else HTTPStatus.INTERNAL_SERVER_ERROR
            return self._send_json({"ok": ok, "message": message}, status=status)
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
    print(f"[macro-agent] Versao: {VERSION}")
    print("[macro-agent] Abra essa URL no navegador e use os botoes.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
