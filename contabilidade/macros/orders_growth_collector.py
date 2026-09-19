"""Coletor separado para a tabela "Orders Growth" do BI interno (BigData/DiDi).

Reaproveita a infraestrutura generica do collector.py (bootstrap do Chrome,
envio em lotes com retry) mas tem extracao propria, porque essa pagina usa
SlickGrid (biblioteca de grid diferente do portal 99Food/gattaran que o
collector.py sabe ler). O SlickGrid usa classes hash por coluna que mudam a
cada carregamento - por isso a extracao aqui le pelo TEXTO do cabecalho e
pela ordem visual das colunas (offsetLeft), nao por classe CSS fixa.
"""

import logging
import time
import uuid
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .collector import build_driver  # reaproveita bootstrap do Chrome, ja testado

logger = logging.getLogger(__name__)

FIELD_ORDER = [
    "shop_id",
    "brand_id",
    "shop_name",
    "grupo_offline",
    "brand_name",
    "bdm_online",
    "bd_username_offline",
    "complete_orders",
    "gmv",
]

TITLE_MATCH_TEXT = "orders growth"

API_TIMEOUT = 180
API_BATCH_SIZE = 500
API_BATCH_SLEEP = 0.3
API_BATCH_MAX_RETRIES = 3
API_BATCH_RETRY_SLEEP = 1.5

# JS injetado: acha o container da tabela "Orders Growth" pelo texto do
# titulo, le os cabecalhos (agrupados por "painel"/pane, ordenados pela
# posicao horizontal local) e le as linhas atualmente renderizadas na tela
# (SlickGrid so renderiza o que esta visivel - por isso a rolagem por fora).
_EXTRACT_JS = r"""
function findContainer(matchText) {
    const all = document.querySelectorAll('p, div, span, h1, h2, h3, h4');
    let titleEl = null;
    for (const el of all) {
        const text = (el.textContent || '').trim().toLowerCase();
        if (text && text.includes(matchText) && text.length < 120) {
            titleEl = el;
            break;
        }
    }
    if (!titleEl) return null;
    let node = titleEl;
    for (let i = 0; i < 14; i++) {
        if (!node.parentElement) break;
        node = node.parentElement;
        if (node.querySelector('.slick-header-column') && node.querySelector('.slick-row')) {
            return node;
        }
    }
    return null;
}

function extractHeaders(container) {
    let panes = Array.from(container.querySelectorAll('[class*="slick-header-columns"]'));
    if (panes.length === 0) panes = [container];
    let headers = [];
    panes.forEach((pane) => {
        const cols = Array.from(pane.querySelectorAll('.slick-header-column'))
            .map((el) => ({ left: el.offsetLeft || 0, text: (el.textContent || '').trim() }))
            .filter((c) => c.text)
            .sort((a, b) => a.left - b.left);
        headers = headers.concat(cols.map((c) => c.text));
    });
    return headers;
}

function extractRows(container) {
    let canvases = Array.from(container.querySelectorAll('[class*="grid-canvas"]'));
    if (canvases.length === 0) canvases = [container];
    const rowsByTop = new Map();
    canvases.forEach((canvasEl, paneIndex) => {
        const rowEls = Array.from(canvasEl.querySelectorAll('.slick-row'));
        rowEls.forEach((rowEl) => {
            const topStyle = rowEl.getAttribute('style') || '';
            const match = topStyle.match(/top:\s*(-?\d+(?:\.\d+)?)px/);
            if (!match) return;
            const topKey = Math.round(parseFloat(match[1]));
            const cells = Array.from(rowEl.querySelectorAll('.slick-cell'))
                .map((el) => ({ left: el.offsetLeft || 0, text: (el.textContent || '').trim() }))
                .sort((a, b) => a.left - b.left)
                .map((c) => c.text);
            if (!rowsByTop.has(topKey)) rowsByTop.set(topKey, {});
            rowsByTop.get(topKey)[paneIndex] = cells;
        });
    });
    const sortedTops = Array.from(rowsByTop.keys()).sort((a, b) => a - b);
    const rows = [];
    sortedTops.forEach((top) => {
        const paneData = rowsByTop.get(top);
        let values = [];
        for (let i = 0; i < canvases.length; i++) {
            values = values.concat(paneData[i] || []);
        }
        if (values.some((v) => v)) rows.push(values);
    });
    return rows;
}

function getScrollInfo(container) {
    const viewport = container.querySelector('[class*="slick-viewport"]');
    if (!viewport) return null;
    return { scrollTop: viewport.scrollTop, scrollHeight: viewport.scrollHeight, clientHeight: viewport.clientHeight };
}

const container = findContainer(arguments[0]);
if (!container) {
    return { ok: false, error: "container-not-found" };
}
return {
    ok: true,
    headers: extractHeaders(container),
    rows: extractRows(container),
    scroll: getScrollInfo(container),
};
"""

_SCROLL_JS = r"""
function findContainer(matchText) {
    const all = document.querySelectorAll('p, div, span, h1, h2, h3, h4');
    let titleEl = null;
    for (const el of all) {
        const text = (el.textContent || '').trim().toLowerCase();
        if (text && text.includes(matchText) && text.length < 120) {
            titleEl = el;
            break;
        }
    }
    if (!titleEl) return null;
    let node = titleEl;
    for (let i = 0; i < 14; i++) {
        if (!node.parentElement) break;
        node = node.parentElement;
        if (node.querySelector('.slick-header-column') && node.querySelector('.slick-row')) {
            return node;
        }
    }
    return null;
}

const container = findContainer(arguments[0]);
if (!container) return false;
const viewports = Array.from(container.querySelectorAll('[class*="slick-viewport"]'));
if (viewports.length === 0) return false;
const step = arguments[1];
viewports.forEach((vp) => { vp.scrollTop = vp.scrollTop + step; });
return true;
"""


def wait_for_orders_growth_table(driver, timeout: int = 180) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".slick-header-column"))
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".slick-row"))
        )
    except TimeoutException:
        return False
    return True


def extract_visible(driver) -> Dict:
    return driver.execute_script(_EXTRACT_JS, TITLE_MATCH_TEXT)


def scroll_table(driver, step_px: int) -> bool:
    return bool(driver.execute_script(_SCROLL_JS, TITLE_MATCH_TEXT, step_px))


def _row_key(row: Sequence[str]) -> str:
    return row[0].strip() if row else ""


def collect_orders_growth_rows(
    driver,
    *,
    max_scroll_steps: int = 200000,
    scroll_step_px: int = 80,
    stall_limit: int = 40,
    settle_seconds: float = 0.9,
    retry_wait_seconds: float = 1.5,
    retries_per_stall: int = 3,
    log_every: int = 50,
) -> Tuple[List[str], List[List[str]]]:
    """Rola a grade virtualizada capturando linhas ate nao aparecer nada novo
    por `stall_limit` tentativas seguidas (fim da tabela) ou bater o limite
    de passos de rolagem.

    A tabela carrega em paginas via AJAX conforme rola (nao e so
    virtualizacao local) - por isso, quando uma rolagem nao traz linha nova,
    tentamos de novo `retries_per_stall` vezes com espera maior antes de
    contar como "parou de verdade", e qualquer sinal de que a altura da
    grade ainda esta crescendo (scrollHeight) zera o contador de estagnacao.
    """
    first = extract_visible(driver)
    if not first.get("ok"):
        raise RuntimeError("Nao encontrei a tabela 'Orders Growth' na pagina. Confirme se ela esta visivel na tela.")

    headers = first.get("headers") or []
    collected: Dict[str, List[str]] = {}
    for row in first.get("rows") or []:
        key = _row_key(row)
        if key:
            collected[key] = row

    last_scroll_height = 0
    scroll_info0 = first.get("scroll") or {}
    if scroll_info0:
        last_scroll_height = scroll_info0.get("scrollHeight") or 0

    stall_count = 0
    for step in range(max_scroll_steps):
        moved = scroll_table(driver, scroll_step_px)
        if not moved:
            break

        new_rows = 0
        snapshot = None
        for attempt in range(retries_per_stall + 1):
            wait_time = settle_seconds if attempt == 0 else retry_wait_seconds
            time.sleep(wait_time)
            snapshot = extract_visible(driver)
            if not snapshot.get("ok"):
                break
            attempt_new_rows = 0
            for row in snapshot.get("rows") or []:
                key = _row_key(row)
                if key and key not in collected:
                    collected[key] = row
                    attempt_new_rows += 1
            new_rows += attempt_new_rows
            scroll_info = snapshot.get("scroll") or {}
            scroll_height = scroll_info.get("scrollHeight") or 0
            grid_still_growing = scroll_height > last_scroll_height
            last_scroll_height = max(last_scroll_height, scroll_height)
            if attempt_new_rows > 0 or grid_still_growing:
                break

        if not snapshot or not snapshot.get("ok"):
            break

        scroll_info = snapshot.get("scroll") or {}
        at_bottom = False
        if scroll_info:
            scroll_top = scroll_info.get("scrollTop") or 0
            scroll_height = scroll_info.get("scrollHeight") or 0
            client_height = scroll_info.get("clientHeight") or 0
            if scroll_top + client_height >= scroll_height - 5:
                at_bottom = True

        if new_rows == 0:
            stall_count += 1
        else:
            stall_count = 0

        if log_every and step % log_every == 0:
            logger.info(
                "Orders Growth: passo %s, linhas coletadas ate agora %s, estagnacao %s/%s",
                step,
                len(collected),
                stall_count,
                stall_limit,
            )

        if at_bottom and stall_count >= 3:
            break
        if stall_count >= stall_limit:
            break

    logger.info("Orders Growth: coleta finalizada com %s linhas.", len(collected))
    return headers, list(collected.values())


def rows_to_dicts(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> List[Dict[str, str]]:
    """Mapeia pela ORDEM das colunas (headers lidos na tela), nao pelo
    indice fixo - se a ordem visual bater com FIELD_ORDER, os nomes tecnicos
    (shop_id, brand_id, ...) sao usados; senao caem como coluna_N generico
    (permitindo revisar manualmente o que veio da pagina).
    """
    field_names = list(FIELD_ORDER)
    if len(headers) >= len(field_names):
        keys = field_names + [f"coluna_extra_{i}" for i in range(len(headers) - len(field_names))]
    else:
        keys = field_names[: len(headers)]

    result = []
    for row in rows:
        item = {}
        for index, key in enumerate(keys):
            item[key] = row[index] if index < len(row) else ""
        result.append(item)
    return result


def send_rows_to_api(
    rows: Sequence[Dict[str, str]],
    *,
    api_url: str,
    api_token: str,
    period: str,
    timeout: int = API_TIMEOUT,
    run_meta: Optional[Dict] = None,
) -> Tuple[int, int]:
    if not api_url:
        logger.warning("api_url nao informado; envio ignorado.")
        return 0, len(rows)

    headers_http = {"Content-Type": "application/json"}
    if api_token:
        headers_http["Authorization"] = f"Bearer {api_token}"

    total = len(rows)
    sent = 0
    batch_size = max(1, API_BATCH_SIZE)
    batch_total = (total + batch_size - 1) // batch_size
    base_meta = dict(run_meta or {})
    base_meta["period"] = period

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        chunk = rows[start:end]
        batch_index = (start // batch_size) + 1
        payload = {
            "rows": chunk,
            "period": period,
            "meta": {**base_meta, "batch_index": batch_index, "batch_total": batch_total},
        }
        ok = False
        for attempt in range(1, API_BATCH_MAX_RETRIES + 1):
            try:
                resp = requests.post(api_url, json=payload, headers=headers_http, timeout=timeout)
                resp.raise_for_status()
                sent += len(chunk)
                ok = True
                break
            except Exception as exc:
                if attempt < API_BATCH_MAX_RETRIES:
                    logger.warning("Falha no lote %s-%s (tentativa %s): %s. Repetindo...", start + 1, end, attempt, exc)
                    time.sleep(API_BATCH_RETRY_SLEEP)
                else:
                    logger.error("Falha definitiva no lote %s-%s: %s", start + 1, end, exc)
        if not ok:
            return sent, total
        if API_BATCH_SLEEP > 0 and end < total:
            time.sleep(API_BATCH_SLEEP)
    return sent, total


def run_orders_growth(
    *,
    headless: bool = False,
    manual_login: bool = True,
    login_timeout: int = 900,
    period: str,
    send_api: bool = True,
    api_url: Optional[str] = None,
    api_token: Optional[str] = None,
    target_url: Optional[str] = None,
    existing_driver=None,
    navigate_to_target: bool = True,
    close_driver: bool = True,
    max_scroll_steps: int = 20000,
):
    driver = None
    owns_driver = existing_driver is None
    sent = 0
    to_send = 0
    try:
        driver = existing_driver if existing_driver is not None else build_driver(headless=headless)
        if navigate_to_target and target_url:
            driver.get(target_url)
        if manual_login:
            logger.info("Faca login/filtro na tela. Aguardando a tabela por ate %ss...", login_timeout)

        if not wait_for_orders_growth_table(driver, timeout=login_timeout):
            return {"rows": [], "headers": [], "collected": 0, "sent": 0, "to_send": 0}

        headers, raw_rows = collect_orders_growth_rows(driver, max_scroll_steps=max_scroll_steps)
        rows = rows_to_dicts(headers, raw_rows)

        if send_api and rows:
            run_meta = {"execution_id": uuid.uuid4().hex, "collected_total": len(rows)}
            sent, to_send = send_rows_to_api(
                rows,
                api_url=api_url or "",
                api_token=api_token or "",
                period=period,
                run_meta=run_meta,
            )
            logger.info("Enviado para API: %s de %s linhas.", sent, to_send)

        return {
            "rows": rows,
            "headers": headers,
            "collected": len(rows),
            "sent": sent,
            "to_send": to_send,
        }
    finally:
        if driver and owns_driver and close_driver:
            try:
                driver.quit()
            except Exception:
                pass
