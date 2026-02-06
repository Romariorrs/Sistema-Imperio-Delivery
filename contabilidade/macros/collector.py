import logging
import os
import re
import time
import unicodedata
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

URL = os.getenv("MACRO_TARGET_URL", "https://gattaran.didi-food.com/v2/gtr_crm/leads/list/all")
MAX_PAGES = int(os.getenv("MACRO_MAX_PAGES", "9999"))

USE_CHROME_PROFILE = os.getenv("USE_CHROME_PROFILE", "false").lower() in {"1", "true", "yes", "on"}
CHROME_USER_DATA_DIR = os.getenv(
    "CHROME_USER_DATA_DIR",
    os.path.join(os.getenv("LOCALAPPDATA", r"C:\\Users"), "Google", "Chrome", "User Data"),
)
CHROME_PROFILE_DIR = os.getenv("CHROME_PROFILE_DIR", "Default")
DEBUGGER_ADDRESS = os.getenv("DEBUGGER_ADDRESS", "").strip()
BINARY_PATH = os.getenv("CHROME_BINARY", r"C:\Program Files\Google\Chrome\Application\chrome.exe")
HEADLESS_DEFAULT = os.getenv("MACRO_HEADLESS", "false").lower() in {"1", "true", "yes", "on"}

API_URL = os.getenv("API_URL", "").strip()
API_TOKEN = os.getenv("API_TOKEN", "").strip()
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "180"))
API_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", "400"))
API_BATCH_SLEEP = float(os.getenv("API_BATCH_SLEEP", "0.4"))

logger = logging.getLogger(__name__)

FIELD_TARGETS = [
    "Cidade",
    "Regiao-alvo",
    "Horario de criacao do lead",
    "Nome do estabelecimento",
    "Nome do representante 99",
    "Status do contrato",
    "Telefone do representante do estabelecimento",
    "Categoria da empresa",
    "Endereco",
]

FALLBACK_INDICES = {
    "Cidade": 2,
    "Regiao-alvo": 3,
    "Horario de criacao do lead": 4,
    "Nome do estabelecimento": 5,
    "Nome do representante 99": 9,
    "Status do contrato": 10,
    "Telefone do representante do estabelecimento": 13,
    "Categoria da empresa": 26,
    "Endereco": 27,
}

CHROME_ARGS = [
    "--remote-allow-origins=*",
    "--disable-extensions",
    "--disable-features=AutomationControlled,Translate",
    "--no-first-run",
    "--no-default-browser-check",
]


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def normalize(text: str) -> str:
    text = (text or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^\w\s-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().casefold()


def map_header_positions(driver) -> Dict[str, int]:
    try:
        header_texts = driver.execute_script(
            """
            const sels = [
              "div.pb-table_header .pb-table_cell",
              "table.pb-table thead tr:first-child th",
              "table.pb-table thead tr:first-child td",
              "[role='columnheader']"
            ];
            const nodes = Array.from(document.querySelectorAll(sels.join(',')));
            return nodes.map(n => (n.innerText || n.textContent || '').trim());
            """
        )
    except Exception:
        header_texts = []

    pos: Dict[str, int] = {}
    for idx, raw in enumerate(header_texts):
        norm = normalize(raw)
        for target in FIELD_TARGETS:
            if normalize(target) == norm and target not in pos:
                pos[target] = idx

    if pos:
        return pos

    headers = driver.find_elements(
        By.XPATH,
        "//div[contains(@class,'pb-table_header')]//div[contains(@class,'pb-table_cell')]"
        " | //table[contains(@class,'pb-table')]//tr[contains(@class,'pb-table_row')][1]/*"
        " | //table//thead//th | //table//thead//td | //div[@role='columnheader']",
    )
    for idx, header in enumerate(headers):
        norm = normalize(header.text.strip())
        for target in FIELD_TARGETS:
            if normalize(target) == norm and target not in pos:
                pos[target] = idx
    return pos


def extract_rows(driver, pos: Dict[str, int]) -> List[List[str]]:
    rows_out: List[List[str]] = []

    try:
        js_rows = driver.execute_script(
            """
            const rows = Array.from(document.querySelectorAll("tr.pb-table_row, table.pb-table tbody tr"));
            return rows.map(r => {
              const directTds = Array.from(r.querySelectorAll(":scope > td"));
              if (directTds.length) {
                return directTds.map(c => (c.innerText || c.textContent || '').trim());
              }
              const directCells = Array.from(r.querySelectorAll(":scope > div.pb-table_cell"));
              if (directCells.length) {
                return directCells.map(c => (c.innerText || c.textContent || '').trim());
              }
              const fallback = Array.from(r.querySelectorAll("td, div.pb-table_cell"));
              return fallback.map(c => (c.innerText || c.textContent || '').trim());
            });
            """
        )
    except Exception:
        js_rows = []

    if js_rows:
        for cells in js_rows:
            picked = []
            for field in FIELD_TARGETS:
                primary = pos.get(field, -1)
                candidates = []
                if primary >= 0:
                    candidates.append(primary)
                if "Telefone do representante" in field:
                    candidates.extend([13, len(cells) - 1])
                elif "Categoria da empresa" in field:
                    candidates.extend([26, 25, 27, 28, 24, 29])
                elif "Endereco" in field:
                    candidates.extend([27, 28, 26, 25, 29, 24])

                txt = ""
                for idx in candidates:
                    if 0 <= idx < len(cells):
                        txt = (cells[idx] or "").strip()
                        if txt:
                            break
                picked.append(txt)
            if any(picked):
                rows_out.append(picked)
        return rows_out

    rows = driver.find_elements(
        By.XPATH,
        "//div[contains(@class,'pb-table_body')]//tr"
        " | //table[contains(@class,'pb-table')]//tbody//tr",
    )
    for row in rows:
        cells = row.find_elements(By.XPATH, "./td")
        if not cells:
            cells = row.find_elements(By.XPATH, "./div[contains(@class,'pb-table_cell')]")
        if not cells:
            cells = row.find_elements(By.XPATH, ".//td | .//div[contains(@class,'pb-table_cell')]")
        if not cells:
            continue
        picked = []
        for field in FIELD_TARGETS:
            idx = pos.get(field, -1)
            if idx >= len(cells) and "Telefone" in field:
                idx = len(cells) - 1
            if 0 <= idx < len(cells):
                txt = cells[idx].text.strip() or (cells[idx].get_attribute("textContent") or "").strip()
            else:
                txt = ""
            picked.append(txt)
        if any(picked):
            rows_out.append(picked)
    return rows_out


def click_next(driver) -> bool:
    disabled = driver.find_elements(
        By.XPATH,
        "//li[contains(@class,'ant-pagination-next') and contains(@class,'ant-pagination-disabled')]"
        " | //button[contains(@class,'btn-next') and @disabled]",
    )
    if disabled:
        return False

    xpaths = [
        "//button[contains(@class,'btn-next') and not(@disabled)]",
        "//i[contains(@class,'pb-icon-arrow-right')]/ancestor::button[not(@disabled)]",
        "//li[contains(@class,'ant-pagination-next')]//button[not(@disabled)]",
        "//li[contains(@class,'ant-pagination-next')]//a",
        "//li[contains(@class,'ant-pagination-next')]//*[contains(@class,'ant-pagination-item-link')]",
    ]
    for xpath in xpaths:
        btns = driver.find_elements(By.XPATH, xpath)
        if not btns:
            continue
        btn = btns[0]
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            return True
        except Exception:
            continue
    return False


def get_active_page(driver) -> str:
    try:
        active = driver.find_element(
            By.XPATH,
            "//li[contains(@class,'ant-pagination-item-active')]/a | "
            "//li[contains(@class,'ant-pagination-item-active')]/button | "
            "//li[contains(@class,'ant-pagination-item-active')]",
        )
        return active.text.strip()
    except Exception:
        return ""


def human_pause(seconds: float = 0.6):
    time.sleep(seconds)


def wait_for_table(driver, timeout: int = 180) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class,'pb-table_body')]//tr | //table[contains(@class,'pb-table')]")
            )
        )
        return True
    except TimeoutException:
        return False


def build_driver(headless: bool = HEADLESS_DEFAULT):
    debugger_address = os.getenv("DEBUGGER_ADDRESS", DEBUGGER_ADDRESS).strip()
    binary_path = os.getenv("CHROME_BINARY", BINARY_PATH)
    use_profile = _env_bool("USE_CHROME_PROFILE", USE_CHROME_PROFILE)
    chrome_user_data_dir = os.getenv("CHROME_USER_DATA_DIR", CHROME_USER_DATA_DIR)
    chrome_profile_dir = os.getenv("CHROME_PROFILE_DIR", CHROME_PROFILE_DIR)

    if debugger_address:
        logger.info("Conectando no Chrome aberto em %s", debugger_address)
        opts = ChromeOptions()
        opts.add_experimental_option("debuggerAddress", debugger_address)
        if binary_path and os.path.isfile(binary_path):
            opts.binary_location = binary_path
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

    opts = ChromeOptions()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--no-sandbox")
    for arg in CHROME_ARGS:
        opts.add_argument(arg)
    if headless:
        opts.add_argument("--headless=new")

    if binary_path and os.path.isfile(binary_path):
        opts.binary_location = binary_path
    if use_profile and os.path.isdir(chrome_user_data_dir):
        opts.add_argument(f"--user-data-dir={chrome_user_data_dir}")
        opts.add_argument(f"--profile-directory={chrome_profile_dir}")
    else:
        localapp = os.getenv("LOCALAPPDATA") or os.getcwd()
        persistent_profile = os.path.join(localapp, "ImperioMacro", "chrome_user_data")
        os.makedirs(persistent_profile, exist_ok=True)
        opts.add_argument(f"--user-data-dir={persistent_profile}")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


def rows_to_dicts(rows: Sequence[Sequence[str]]) -> List[Dict[str, str]]:
    return [{FIELD_TARGETS[i]: (row[i] if i < len(row) else "") for i in range(len(FIELD_TARGETS))} for row in rows]


def send_rows_to_api(
    rows: Sequence[Sequence[str]],
    api_url: Optional[str] = None,
    api_token: Optional[str] = None,
    timeout: int = API_TIMEOUT,
) -> Tuple[int, int]:
    endpoint = (api_url or API_URL).strip()
    if not endpoint:
        logger.warning("API_URL nao informado; envio ignorado.")
        return 0, len(rows)

    payload = rows_to_dicts(rows)
    headers = {"Content-Type": "application/json"}
    token = (api_token or API_TOKEN).strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    total = len(rows)
    sent = 0
    batch_size = max(1, API_BATCH_SIZE)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        chunk = payload[start:end]
        try:
            resp = requests.post(endpoint, json=chunk, headers=headers, timeout=timeout)
            resp.raise_for_status()
            sent += len(chunk)
        except Exception as exc:
            logger.error("Falha no envio para API no lote %s-%s: %s", start + 1, end, exc)
            return sent, total
        if API_BATCH_SLEEP > 0 and end < total:
            time.sleep(API_BATCH_SLEEP)
    return sent, total


def run(
    *,
    headless: bool = HEADLESS_DEFAULT,
    manual_login: bool = True,
    login_timeout: int = 180,
    max_pages: int = MAX_PAGES,
    send_api: bool = True,
    api_url: Optional[str] = None,
    api_token: Optional[str] = None,
    target_url: Optional[str] = None,
) -> List[List[str]]:
    return run_with_metrics(
        headless=headless,
        manual_login=manual_login,
        login_timeout=login_timeout,
        max_pages=max_pages,
        send_api=send_api,
        api_url=api_url,
        api_token=api_token,
        target_url=target_url,
    )["rows"]


def run_with_metrics(
    *,
    headless: bool = HEADLESS_DEFAULT,
    manual_login: bool = True,
    login_timeout: int = 180,
    max_pages: int = MAX_PAGES,
    send_api: bool = True,
    api_url: Optional[str] = None,
    api_token: Optional[str] = None,
    target_url: Optional[str] = None,
    existing_driver=None,
    navigate_to_target: bool = True,
    close_driver: bool = True,
):
    driver = None
    owns_driver = existing_driver is None
    all_rows: List[List[str]] = []
    sent = 0
    to_send = 0
    try:
        driver = existing_driver if existing_driver is not None else build_driver(headless=headless)
        final_target_url = (target_url or os.getenv("MACRO_TARGET_URL", URL)).strip() or URL
        if navigate_to_target:
            driver.get(final_target_url)
        if manual_login:
            logger.info("Faca login e aplique filtro na tela. Aguardando tabela por ate %ss...", login_timeout)

        if not wait_for_table(driver, timeout=login_timeout):
            logger.error("Tabela nao encontrada dentro do timeout.")
            return {
                "rows": [],
                "collected": 0,
                "deduplicated": 0,
                "sent": 0,
                "to_send": 0,
            }

        pos = map_header_positions(driver)
        if len(pos) < 2:
            logger.warning("Cabecalhos nao reconhecidos, usando fallback de indices.")
            pos = FALLBACK_INDICES

        page = 0
        while True:
            page += 1
            rows = extract_rows(driver, pos)
            logger.info("Pagina %s: %s linhas", page, len(rows))
            all_rows.extend(rows)
            if page >= max_pages:
                break
            prev_active = get_active_page(driver)
            if not click_next(driver):
                break
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//li[contains(@class,'ant-pagination-item-active')]"))
                )
            except TimeoutException:
                pass
            for _ in range(10):
                curr = get_active_page(driver)
                if curr and curr != prev_active:
                    break
                human_pause(0.3)
            human_pause()

        dedup_rows: List[List[str]] = []
        seen = set()
        for row in all_rows:
            key = tuple(row)
            if key in seen:
                continue
            seen.add(key)
            dedup_rows.append(row)

        if send_api and dedup_rows:
            sent, to_send = send_rows_to_api(dedup_rows, api_url=api_url, api_token=api_token)
            logger.info("Enviado para API: %s de %s linhas.", sent, to_send)

        return {
            "rows": dedup_rows,
            "collected": len(all_rows),
            "deduplicated": len(dedup_rows),
            "sent": sent,
            "to_send": to_send,
        }
    finally:
        if (
            driver
            and owns_driver
            and close_driver
            and not os.getenv("DEBUGGER_ADDRESS", DEBUGGER_ADDRESS).strip()
        ):
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    result = run_with_metrics()
    logger.info("Concluido. Coletadas=%s | Deduplicadas=%s", result["collected"], result["deduplicated"])
