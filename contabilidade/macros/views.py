import csv
import io
import json
import logging
import secrets
from datetime import datetime, timedelta
from pathlib import Path
import zipfile
from typing import Iterable
from urllib.parse import urlencode

from django.conf import settings
from django.core.cache import cache
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from .models import MacroLead, MacroRun
from .services import EXPORT_COLUMNS, upsert_rows

logger = logging.getLogger(__name__)


def _staff_access(user):
    return user.is_staff or user.is_superuser


def _apply_filters(request=None, queryset=None, params=None):
    queryset = queryset or MacroLead.objects.all()
    params = params or (request.GET if request is not None else {})
    q = (params.get("q") or "").strip()
    q_digits = "".join(char for char in q if char.isdigit())
    city = (params.get("city") or "").strip()
    contract_status = (params.get("contract_status") or "").strip()
    business_99_status = (params.get("business_99_status") or "").strip()
    company_category = (params.get("company_category") or "").strip()
    blocked = (params.get("blocked") or "").strip().lower()
    phone_dup = (params.get("phone_dup") or "").strip().lower()
    lead_date_from = (params.get("lead_date_from") or "").strip()
    lead_date_to = (params.get("lead_date_to") or "").strip()
    duplicate_phone_norms = (
        MacroLead.objects.exclude(representative_phone_norm="")
        .values("representative_phone_norm")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
        .values_list("representative_phone_norm", flat=True)
    )

    if q:
        text_filter = (
            Q(city__icontains=q)
            | Q(target_region__icontains=q)
            | Q(establishment_name__icontains=q)
            | Q(representative_name__icontains=q)
            | Q(contract_status__icontains=q)
            | Q(business_99_status__icontains=q)
            | Q(representative_phone__icontains=q)
            | Q(company_category__icontains=q)
            | Q(address__icontains=q)
        )
        if q_digits:
            text_filter |= Q(representative_phone_norm__icontains=q_digits)
        queryset = queryset.filter(text_filter)
    if city:
        queryset = queryset.filter(city=city)
    if contract_status:
        queryset = queryset.filter(contract_status=contract_status)
    if business_99_status:
        queryset = queryset.filter(business_99_status=business_99_status)
    if company_category:
        queryset = queryset.filter(company_category=company_category)
    if blocked == "yes":
        queryset = queryset.filter(is_blocked_number=True)
    elif blocked == "no":
        queryset = queryset.filter(is_blocked_number=False)
    if phone_dup == "duplicates":
        queryset = queryset.filter(representative_phone_norm__in=duplicate_phone_norms)
    elif phone_dup == "unique":
        queryset = queryset.exclude(representative_phone_norm="").exclude(
            representative_phone_norm__in=duplicate_phone_norms
        )
    elif phone_dup == "empty":
        queryset = queryset.filter(representative_phone_norm="")
    if lead_date_from:
        queryset = queryset.filter(lead_created_at__date__gte=lead_date_from)
    if lead_date_to:
        queryset = queryset.filter(lead_created_at__date__lte=lead_date_to)
    return queryset.order_by("-last_seen_at", "-id")


def _safe_macro_redirect(post_data):
    target = (post_data.get("next") or "").strip()
    if target == "collect":
        return redirect("macro_collect")
    return redirect("macro_list")


def _filtered_delete_redirect(params):
    base = reverse("macro_list")
    cleaned = {
        k: (params.get(k) or "").strip()
        for k in (
            "q",
            "city",
            "contract_status",
            "business_99_status",
            "company_category",
            "blocked",
            "phone_dup",
            "lead_date_from",
            "lead_date_to",
        )
    }
    cleaned = {k: v for k, v in cleaned.items() if v}
    if not cleaned:
        return redirect("macro_list")
    return redirect(f"{base}?{urlencode(cleaned)}")


def _lead_sources():
    return (
        MacroLead.objects.exclude(source="")
        .values_list("source", flat=True)
        .distinct()
        .order_by("source")
    )


def _macro_agent_version_meta():
    version = (settings.MACRO_AGENT_VERSION or "").strip() or "v0.0.0"
    exe_path = Path(settings.MACRO_LOCAL_AGENT_EXE_PATH)
    build = ""
    if exe_path.exists():
        modified_at = datetime.fromtimestamp(exe_path.stat().st_mtime)
        build = modified_at.strftime("%Y%m%d-%H%M")
    label = f"{version}-{build}" if build else version
    return {"version": version, "build": build, "label": label}


def _staff_or_token(request):
    if request.user.is_authenticated and _staff_access(request.user):
        return True

    configured = settings.MACRO_API_TOKEN
    if not configured:
        return False

    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        return False
    provided = auth.split(" ", 1)[1].strip()
    return bool(provided) and secrets.compare_digest(provided, configured)


def _client_ip(request) -> str:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "").strip()


def _ip_allowed(request) -> bool:
    allowed_ips = set(getattr(settings, "MACRO_API_ALLOWED_IPS", []))
    if not allowed_ips:
        return True
    return _client_ip(request) in allowed_ips


def _rate_limited(request) -> bool:
    limit = int(getattr(settings, "MACRO_API_RATE_LIMIT_PER_MINUTE", 60))
    if limit <= 0:
        return False
    ip = _client_ip(request) or "unknown"
    cache_key = f"macro_api_rl:{ip}"
    current = cache.get(cache_key)
    if current is None:
        cache.set(cache_key, 1, timeout=60)
        return False
    if int(current) >= limit:
        return True
    try:
        cache.incr(cache_key)
    except Exception:
        cache.set(cache_key, int(current) + 1, timeout=60)
    return False


def _rows_from_json_payload(payload) -> Iterable[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            return payload["rows"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
    return []


@login_required
@user_passes_test(_staff_access)
def macro_list(request):
    version_meta = _macro_agent_version_meta()
    all_queryset = MacroLead.objects.all()
    filtered_queryset = _apply_filters(request, queryset=all_queryset)
    paginator = Paginator(filtered_queryset, 100)
    page_obj = paginator.get_page(request.GET.get("page"))
    query_params = request.GET.copy()
    query_params.pop("page", None)

    stats = all_queryset.aggregate(
        total_records=Count("id"),
        total_cities=Count("city", distinct=True, filter=~Q(city="")),
        phones_filled=Count("id", filter=~Q(representative_phone_norm="")),
        phones_unique=Count("representative_phone_norm", distinct=True, filter=~Q(representative_phone_norm="")),
        blocked_numbers=Count("id", filter=Q(is_blocked_number=True)),
        blocked_numbers_unique=Count(
            "representative_phone_norm",
            distinct=True,
            filter=Q(is_blocked_number=True) & ~Q(representative_phone_norm=""),
        ),
        total_categories=Count("company_category", distinct=True, filter=~Q(company_category="")),
    )
    duplicate_phone_stats = (
        all_queryset.exclude(representative_phone_norm="")
        .values("representative_phone_norm")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
    )
    stats["duplicate_phone_numbers"] = duplicate_phone_stats.count()
    stats["duplicate_phone_leads"] = sum(row["total"] for row in duplicate_phone_stats)

    page_phone_norms = {
        (item.representative_phone_norm or "").strip()
        for item in page_obj.object_list
        if (item.representative_phone_norm or "").strip()
    }
    page_duplicate_phone_norms = set()
    if page_phone_norms:
        page_duplicate_phone_norms = set(
            all_queryset.filter(representative_phone_norm__in=page_phone_norms)
            .values("representative_phone_norm")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .values_list("representative_phone_norm", flat=True)
        )
    city_breakdown = (
        all_queryset.exclude(city="")
        .values("city")
        .annotate(total=Count("id"))
        .order_by("-total", "city")[:10]
    )
    category_breakdown = (
        all_queryset.exclude(company_category="")
        .values("company_category")
        .annotate(total=Count("id"))
        .order_by("-total", "company_category")[:10]
    )
    status_breakdown = (
        all_queryset.exclude(contract_status="")
        .values("contract_status")
        .annotate(total=Count("id"))
        .order_by("-total", "contract_status")[:10]
    )
    business_99_breakdown = (
        all_queryset.exclude(business_99_status="")
        .values("business_99_status")
        .annotate(total=Count("id"))
        .order_by("-total", "business_99_status")[:10]
    )
    last_capture_at = all_queryset.order_by("-last_seen_at").values_list("last_seen_at", flat=True).first()
    recent_runs = MacroRun.objects.all()[:12]
    last_success_run = MacroRun.objects.filter(status="success").first()

    context = {
        "active_tab": "database",
        "page_obj": page_obj,
        "filtered_count": filtered_queryset.count(),
        "cities": MacroLead.objects.exclude(city="").values_list("city", flat=True).distinct().order_by("city"),
        "contract_statuses": MacroLead.objects.exclude(contract_status="")
        .values_list("contract_status", flat=True)
        .distinct()
        .order_by("contract_status"),
        "business_99_statuses": MacroLead.objects.exclude(business_99_status="")
        .values_list("business_99_status", flat=True)
        .distinct()
        .order_by("business_99_status"),
        "categories": MacroLead.objects.exclude(company_category="")
        .values_list("company_category", flat=True)
        .distinct()
        .order_by("company_category"),
        "api_import_url": request.build_absolute_uri(reverse("macro_api_import")),
        "macro_target_url": settings.MACRO_TARGET_URL,
        "token_configured": bool(settings.MACRO_API_TOKEN),
        "filter_querystring": query_params.urlencode(),
        "recent_runs": recent_runs,
        "local_agent_url": settings.MACRO_LOCAL_AGENT_URL,
        "macro_agent_version": version_meta["version"],
        "macro_agent_build": version_meta["build"],
        "macro_agent_label": version_meta["label"],
        "stats": stats,
        "page_duplicate_phone_norms": page_duplicate_phone_norms,
        "last_capture_at": last_capture_at,
        "last_success_run": last_success_run,
        "city_breakdown": city_breakdown,
        "category_breakdown": category_breakdown,
        "status_breakdown": status_breakdown,
        "business_99_breakdown": business_99_breakdown,
        "sources": _lead_sources(),
    }
    return render(request, "macros/list.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_collect(request):
    version_meta = _macro_agent_version_meta()
    last_success_run = MacroRun.objects.filter(status="success").first()
    last_error_run = MacroRun.objects.filter(status="error").first()
    runs_24h = MacroRun.objects.filter(started_at__gte=timezone.now() - timedelta(hours=24))
    exe_path = Path(settings.MACRO_LOCAL_AGENT_EXE_PATH)
    context = {
        "active_tab": "collect",
        "api_import_url": request.build_absolute_uri(reverse("macro_api_import")),
        "macro_target_url": settings.MACRO_TARGET_URL,
        "token_configured": bool(settings.MACRO_API_TOKEN),
        "recent_runs": MacroRun.objects.all()[:12],
        "local_agent_url": settings.MACRO_LOCAL_AGENT_URL,
        "local_agent_exe_available": exe_path.exists(),
        "macro_agent_version": version_meta["version"],
        "macro_agent_build": version_meta["build"],
        "macro_agent_label": version_meta["label"],
        "total_records": MacroLead.objects.count(),
        "last_capture_at": MacroLead.objects.order_by("-last_seen_at").values_list("last_seen_at", flat=True).first(),
        "last_success_run": last_success_run,
        "last_error_run": last_error_run,
        "runs_24h_total": runs_24h.count(),
        "runs_24h_error": runs_24h.filter(status="error").count(),
        "sources": _lead_sources(),
    }
    return render(request, "macros/collect.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_export_csv(request):
    queryset = _apply_filters(request)
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="macro_leads.csv"'

    writer = csv.writer(response)
    writer.writerow([label for _, label in EXPORT_COLUMNS] + ["Source", "Primeira captura", "Ultima captura"])
    for item in queryset:
        row_values = []
        for field, _ in EXPORT_COLUMNS:
            value = getattr(item, field, "")
            if field == "lead_created_at" and value:
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            row_values.append(value)
        writer.writerow(
            row_values
            + [
                item.source,
                item.first_seen_at.strftime("%Y-%m-%d %H:%M:%S"),
                item.last_seen_at.strftime("%Y-%m-%d %H:%M:%S"),
            ]
        )
    return response


@login_required
@user_passes_test(_staff_access)
def macro_export_xlsx(request):
    from openpyxl import Workbook

    queryset = _apply_filters(request)
    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="macro_leads.xlsx"'

    wb = Workbook()
    ws = wb.active
    ws.title = "Macro Leads"
    ws.append([label for _, label in EXPORT_COLUMNS] + ["Source", "Primeira captura", "Ultima captura"])
    for item in queryset:
        row_values = []
        for field, _ in EXPORT_COLUMNS:
            value = getattr(item, field, "")
            if field == "lead_created_at" and value:
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            row_values.append(value)
        ws.append(
            row_values
            + [
                item.source,
                item.first_seen_at.strftime("%Y-%m-%d %H:%M:%S"),
                item.last_seen_at.strftime("%Y-%m-%d %H:%M:%S"),
            ]
        )
    wb.save(response)
    return response


@login_required
@user_passes_test(_staff_access)
def macro_import_csv(request):
    if request.method != "POST":
        return redirect("macro_list")

    run_log = MacroRun.objects.create(
        run_type="csv",
        status="running",
        source="csv",
        triggered_by=request.user if request.user.is_authenticated else None,
        request_ip=_client_ip(request) or None,
    )

    upload = request.FILES.get("file")
    if not upload:
        run_log.status = "error"
        run_log.message = "Arquivo CSV nao enviado."
        run_log.finished_at = timezone.now()
        run_log.save(update_fields=["status", "message", "finished_at"])
        messages.error(request, "Selecione um arquivo CSV para importar.")
        return redirect("macro_list")

    raw_bytes = upload.read()
    decoded = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            decoded = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue

    if decoded is None:
        run_log.status = "error"
        run_log.message = "Nao foi possivel decodificar o CSV."
        run_log.finished_at = timezone.now()
        run_log.save(update_fields=["status", "message", "finished_at"])
        messages.error(request, "Nao foi possivel ler o arquivo CSV.")
        return redirect("macro_list")

    reader = csv.DictReader(io.StringIO(decoded))
    if not reader.fieldnames:
        run_log.status = "error"
        run_log.message = "CSV sem cabecalho."
        run_log.finished_at = timezone.now()
        run_log.save(update_fields=["status", "message", "finished_at"])
        messages.error(request, "CSV sem cabecalho.")
        return redirect("macro_list")

    result = upsert_rows(reader, default_source="csv")
    run_log.status = "success"
    run_log.finished_at = timezone.now()
    run_log.total_collected = result["processed"]
    run_log.total_received = result["processed"]
    run_log.created_count = result["created"]
    run_log.updated_count = result["updated"]
    run_log.ignored_count = result["ignored"]
    run_log.invalid_count = result["invalid"]
    run_log.message = "Importacao CSV concluida."
    run_log.save(
        update_fields=[
            "status",
            "finished_at",
            "total_collected",
            "total_received",
            "created_count",
            "updated_count",
            "ignored_count",
            "invalid_count",
            "message",
        ]
    )
    messages.success(
        request,
        (
            f"Importacao concluida. Processadas: {result['processed']} | "
            f"Novas: {result['created']} | Atualizadas: {result['updated']} | "
            f"Ignoradas: {result['ignored']} | Invalidas: {result['invalid']}"
        ),
    )
    return redirect("macro_list")


@login_required
@user_passes_test(_staff_access)
def macro_delete_filtered(request):
    if request.method != "POST":
        return redirect("macro_list")
    if (request.POST.get("confirm_text") or "").strip().upper() != "EXCLUIR":
        messages.error(request, 'Para excluir dados filtrados, digite "EXCLUIR".')
        return _filtered_delete_redirect(request.POST)

    queryset = _apply_filters(queryset=MacroLead.objects.all(), params=request.POST)
    deleted_count, _ = queryset.delete()
    messages.success(request, f"{deleted_count} registro(s) filtrado(s) excluido(s) com sucesso.")
    return redirect("macro_list")


@login_required
@user_passes_test(_staff_access)
def macro_delete_all(request):
    if request.method != "POST":
        return redirect("macro_list")
    if (request.POST.get("confirm_text") or "").strip().upper() != "APAGAR TUDO":
        messages.error(request, 'Para excluir toda a base, digite "APAGAR TUDO".')
        return _safe_macro_redirect(request.POST)

    deleted_count, _ = MacroLead.objects.all().delete()
    messages.success(request, f"Base limpa com sucesso. {deleted_count} registro(s) removido(s).")
    return _safe_macro_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_delete_runs(request):
    if request.method != "POST":
        return redirect("macro_collect")
    if (request.POST.get("confirm_text") or "").strip().upper() != "LIMPAR HISTORICO":
        messages.error(request, 'Para limpar o historico, digite "LIMPAR HISTORICO".')
        return _safe_macro_redirect(request.POST)

    deleted_count, _ = MacroRun.objects.all().delete()
    messages.success(request, f"Historico limpo. {deleted_count} execucao(oes) removida(s).")
    return _safe_macro_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_delete_source(request):
    if request.method != "POST":
        return redirect("macro_list")
    if (request.POST.get("confirm_text") or "").strip().upper() != "EXCLUIR BASE":
        messages.error(request, 'Para excluir base especifica, digite "EXCLUIR BASE".')
        return _safe_macro_redirect(request.POST)

    source = (request.POST.get("source") or "").strip()
    if not source:
        messages.error(request, "Selecione uma fonte para excluir.")
        return _safe_macro_redirect(request.POST)

    city = (request.POST.get("city") or "").strip()
    queryset = MacroLead.objects.filter(source=source)
    if city:
        queryset = queryset.filter(city__iexact=city)
    deleted_count, _ = queryset.delete()

    detail = f"fonte '{source}'"
    if city:
        detail += f" e cidade '{city}'"
    messages.success(request, f"Base especifica removida ({detail}). {deleted_count} registro(s) excluido(s).")
    return _safe_macro_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_delete_blocked(request):
    if request.method != "POST":
        return redirect("macro_list")
    if (request.POST.get("confirm_text") or "").strip().upper() != "EXCLUIR BLOQUEADOS":
        messages.error(request, 'Para excluir bloqueados, digite "EXCLUIR BLOQUEADOS".')
        return _filtered_delete_redirect(request.POST)

    queryset = _apply_filters(queryset=MacroLead.objects.all(), params=request.POST).filter(is_blocked_number=True)
    deleted_count, _ = queryset.delete()
    messages.success(request, f"{deleted_count} lead(s) bloqueado(s) excluido(s).")
    return _filtered_delete_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_toggle_phone_block(request, lead_id: int, blocked: bool):
    if request.method != "POST":
        return redirect("macro_list")

    lead = get_object_or_404(MacroLead, id=lead_id)
    now = timezone.now()
    phone_norm = (lead.representative_phone_norm or "").strip()
    if phone_norm:
        affected = MacroLead.objects.filter(representative_phone_norm=phone_norm)
    else:
        affected = MacroLead.objects.filter(id=lead.id)

    updated_count = affected.update(is_blocked_number=blocked, last_seen_at=now)
    if blocked:
        messages.success(request, f"Numero marcado como bloqueado. Leads afetados: {updated_count}.")
    else:
        messages.success(request, f"Numero removido dos bloqueados. Leads afetados: {updated_count}.")
    return _filtered_delete_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_delete_run_item(request, run_id: int):
    if request.method != "POST":
        return redirect("macro_collect")
    deleted_count, _ = MacroRun.objects.filter(id=run_id).delete()
    if deleted_count:
        messages.success(request, "Execucao removida do historico.")
    else:
        messages.error(request, "Execucao nao encontrada.")
    return _safe_macro_redirect(request.POST)


@login_required
@user_passes_test(_staff_access)
def macro_download_local_agent_py(request):
    agent_path = Path(settings.BASE_DIR) / "local_macro_agent.py"
    if not agent_path.exists():
        return HttpResponse("Arquivo local_macro_agent.py nao encontrado.", status=404)

    content = agent_path.read_text(encoding="utf-8")
    response = HttpResponse(content, content_type="text/x-python")
    response["Content-Disposition"] = 'attachment; filename="local_macro_agent.py"'
    return response


@login_required
@user_passes_test(_staff_access)
def macro_download_local_agent_bat(request):
    bat_content = (
        "@echo off\r\n"
        "setlocal\r\n"
        "cd /d \"%~dp0\"\r\n"
        "if exist .venv\\Scripts\\python.exe (\r\n"
        "  .venv\\Scripts\\python.exe local_macro_agent.py\r\n"
        ") else (\r\n"
        "  where py >nul 2>nul\r\n"
        "  if %ERRORLEVEL%==0 (\r\n"
        "    py -3 local_macro_agent.py\r\n"
        "  ) else (\r\n"
        "    python local_macro_agent.py\r\n"
        "  )\r\n"
        ")\r\n"
        "pause\r\n"
    )
    response = HttpResponse(bat_content, content_type="text/plain; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="iniciar_coletor_local.bat"'
    return response


@login_required
@user_passes_test(_staff_access)
def macro_download_local_agent_mac(request):
    base_dir = Path(settings.BASE_DIR)
    agent_path = base_dir / "local_macro_agent.py"
    collector_path = base_dir / "contabilidade" / "macros" / "collector.py"

    if not agent_path.exists() or not collector_path.exists():
        return HttpResponse("Arquivos do coletor nao encontrados.", status=404)

    mac_launcher = """#!/bin/bash
set -e
cd "$(dirname "$0")"
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 nao encontrado. Instale via Homebrew: brew install python"
  exit 1
fi
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install selenium webdriver-manager requests
python local_macro_agent.py
"""
    readme = """Coletor Local para macOS

1) Extraia este .zip em uma pasta.
2) No Finder, clique com o botao direito em iniciar_coletor_mac.command e escolha Abrir.
3) Se o macOS bloquear, va em Ajustes > Privacidade e Seguranca > Abrir Mesmo Assim.
4) O coletor abrira em http://127.0.0.1:8765
"""

    content = io.BytesIO()
    with zipfile.ZipFile(content, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("local_macro_agent.py", agent_path.read_text(encoding="utf-8"))
        zf.writestr("contabilidade/__init__.py", "")
        zf.writestr("contabilidade/macros/__init__.py", "")
        zf.writestr(
            "contabilidade/macros/collector.py",
            collector_path.read_text(encoding="utf-8"),
        )
        info = zipfile.ZipInfo("iniciar_coletor_mac.command")
        info.external_attr = 0o755 << 16
        zf.writestr(info, mac_launcher)
        zf.writestr("README_MAC.txt", readme)

    response = HttpResponse(content.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = 'attachment; filename="ColetorMacro-macOS.zip"'
    return response


@login_required
@user_passes_test(_staff_access)
def macro_download_local_agent_exe(request):
    exe_path = Path(settings.MACRO_LOCAL_AGENT_EXE_PATH)
    if not exe_path.exists():
        return HttpResponse("Arquivo ColetorMacro.exe nao encontrado.", status=404)
    content = exe_path.read_bytes()
    response = HttpResponse(content, content_type="application/octet-stream")
    response["Content-Disposition"] = 'attachment; filename="ColetorMacro.exe"'
    return response


@csrf_exempt
def macro_api_import(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)
    if not _staff_or_token(request):
        return JsonResponse({"ok": False, "detail": "Unauthorized"}, status=401)
    if not _ip_allowed(request):
        return JsonResponse({"ok": False, "detail": "IP not allowed"}, status=403)
    if _rate_limited(request):
        return JsonResponse({"ok": False, "detail": "Rate limit exceeded"}, status=429)

    run_log = MacroRun.objects.create(
        run_type="api",
        status="running",
        source="api",
        triggered_by=request.user if request.user.is_authenticated else None,
        request_ip=_client_ip(request) or None,
    )

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        run_log.status = "error"
        run_log.message = "JSON invalido."
        run_log.finished_at = timezone.now()
        run_log.save(update_fields=["status", "message", "finished_at"])
        return JsonResponse({"ok": False, "detail": "Invalid JSON"}, status=400)

    rows = _rows_from_json_payload(payload)
    if not rows:
        run_log.status = "error"
        run_log.message = "Payload sem linhas."
        run_log.finished_at = timezone.now()
        run_log.save(update_fields=["status", "message", "finished_at"])
        return JsonResponse({"ok": False, "detail": "Payload sem linhas"}, status=400)

    try:
        result = upsert_rows(rows, default_source="api")
    except Exception:
        logger.exception("Falha interna no import da macro API")
        run_log.status = "error"
        run_log.finished_at = timezone.now()
        run_log.total_collected = len(rows)
        run_log.message = "Erro interno ao processar lote da API."
        run_log.save(update_fields=["status", "finished_at", "total_collected", "message"])
        return JsonResponse({"ok": False, "detail": "Internal processing error"}, status=500)

    run_log.status = "success"
    run_log.finished_at = timezone.now()
    run_log.total_collected = result["processed"]
    run_log.total_received = result["processed"]
    run_log.created_count = result["created"]
    run_log.updated_count = result["updated"]
    run_log.ignored_count = result["ignored"]
    run_log.invalid_count = result["invalid"]
    run_log.message = "Importacao API concluida."
    run_log.save(
        update_fields=[
            "status",
            "finished_at",
            "total_collected",
            "total_received",
            "created_count",
            "updated_count",
            "ignored_count",
            "invalid_count",
            "message",
        ]
    )
    return JsonResponse({"ok": True, **result})
