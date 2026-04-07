import csv
import io
import json
import logging
import re
import secrets
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
import zipfile
from typing import Iterable
from urllib.parse import urlencode

from django.conf import settings
from django.core.cache import cache
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.db import connection
from django.db.models import Count, Q
from django.db.utils import OperationalError, ProgrammingError
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from .models import MacroLead, MacroRun
from .services import EXPORT_COLUMNS, upsert_rows

logger = logging.getLogger(__name__)

EXPORT_EXTRA_COLUMNS = (
    ("source", "Fonte"),
    ("first_seen_at", "Primeira captura"),
    ("last_seen_at", "Ultima captura"),
    ("exported_at", "Exportado em"),
    ("export_batch_id", "Lote exportacao"),
)
OPTIONAL_DB_FIELDS = {
    "representative_phone_norm",
    "lead_created_at",
    "is_blocked_number",
    "business_99_status",
    "store_id",
    "signatory_id",
    "exported_at",
    "export_batch_id",
}
OPTIONAL_RUN_FIELDS = {"pages_processed", "execution_id", "total_deduplicated"}
EXPORT_TRACKING_FIELDS = {"exported_at", "export_batch_id"}
EXPORT_FIELD_CHOICES = EXPORT_COLUMNS + EXPORT_EXTRA_COLUMNS
MAX_EXPORT_LIMIT = 50000
MAX_CITY_REPORT_EXPORT = 5000


def _staff_access(user):
    return user.is_staff or user.is_superuser


@lru_cache(maxsize=1)
def _macrolead_db_columns():
    table_name = MacroLead._meta.db_table
    try:
        with connection.cursor() as cursor:
            return {column.name for column in connection.introspection.get_table_description(cursor, table_name)}
    except (OperationalError, ProgrammingError):
        logger.exception("Nao foi possivel introspectar a tabela de MacroLead.")
        return set()


def _macrolead_has_columns(*names: str) -> bool:
    columns = _macrolead_db_columns()
    return set(names).issubset(columns)


def _macrolead_table_ready() -> bool:
    return bool(_macrolead_db_columns())


def _export_tracking_enabled() -> bool:
    return _macrolead_has_columns("exported_at", "export_batch_id")


def _base_macrolead_queryset():
    if not _macrolead_table_ready():
        return MacroLead.objects.none()
    queryset = MacroLead.objects.all()
    missing_fields = [field for field in OPTIONAL_DB_FIELDS if field not in _macrolead_db_columns()]
    if missing_fields:
        queryset = queryset.defer(*missing_fields)
    return queryset


@lru_cache(maxsize=1)
def _macrorun_db_columns():
    table_name = MacroRun._meta.db_table
    try:
        with connection.cursor() as cursor:
            return {column.name for column in connection.introspection.get_table_description(cursor, table_name)}
    except (OperationalError, ProgrammingError):
        logger.exception("Nao foi possivel introspectar a tabela de MacroRun.")
        return set()


def _macrorun_has_columns(*names: str) -> bool:
    columns = _macrorun_db_columns()
    return set(names).issubset(columns)


def _macrorun_table_ready() -> bool:
    return bool(_macrorun_db_columns())


def _base_macrorun_queryset():
    if not _macrorun_table_ready():
        return MacroRun.objects.none()
    queryset = MacroRun.objects.all()
    missing_fields = [field for field in OPTIONAL_RUN_FIELDS if field not in _macrorun_db_columns()]
    if missing_fields:
        queryset = queryset.defer(*missing_fields)
    return queryset


def _available_export_field_choices():
    choices = []
    for field, label in EXPORT_FIELD_CHOICES:
        if field in OPTIONAL_DB_FIELDS and field not in _macrolead_db_columns():
            continue
        choices.append((field, label))
    return tuple(choices)


def _available_export_field_labels():
    return {field: label for field, label in _available_export_field_choices()}


def _default_export_fields():
    return [field for field, _ in _available_export_field_choices()]


def _missing_representative_q() -> Q:
    return (
        Q(representative_name="")
        | Q(representative_name="-")
        | Q(representative_name="--")
        | Q(representative_name__iexact="sem representante")
        | Q(representative_name__iexact="nao informado")
        | Q(representative_name__iexact="não informado")
    )


def _parse_ddd_filter(raw_value: str) -> list[str]:
    ddds = []
    for token in re.split(r"[,\s;]+", str(raw_value or "").strip()):
        digits = "".join(char for char in token if char.isdigit())
        if not digits:
            continue
        if digits.startswith("55") and len(digits) >= 4:
            digits = digits[2:4]
        elif len(digits) > 2:
            digits = digits[:2]
        if len(digits) != 2 or digits in ddds:
            continue
        ddds.append(digits)
    return ddds


def _coerce_date_range(date_from: str, date_to: str) -> tuple[str, str]:
    start = (date_from or "").strip()
    end = (date_to or "").strip()
    if not start or not end:
        return start, end
    if start > end:
        return end, start
    return start, end


def _apply_filters(request=None, queryset=None, params=None):
    queryset = queryset if queryset is not None else _base_macrolead_queryset()
    params = params or (request.GET if request is not None else {})
    q = (params.get("q") or "").strip()
    q_digits = "".join(char for char in q if char.isdigit())
    ddd_filter = (params.get("ddd_filter") or "").strip()
    city = (params.get("city") or "").strip()
    contract_status = (params.get("contract_status") or "").strip()
    business_99_status = (params.get("business_99_status") or "").strip()
    company_category = (params.get("company_category") or "").strip()
    representative_presence = (params.get("representative_presence") or "").strip().lower()
    blocked = (params.get("blocked") or "").strip().lower()
    phone_dup = (params.get("phone_dup") or "").strip().lower()
    export_status = (params.get("export_status") or "").strip().lower()
    lead_date_from = (params.get("lead_date_from") or "").strip()
    lead_date_to = (params.get("lead_date_to") or "").strip()
    lead_date_from, lead_date_to = _coerce_date_range(lead_date_from, lead_date_to)
    phone_norm_enabled = _macrolead_has_columns("representative_phone_norm")
    blocked_enabled = _macrolead_has_columns("is_blocked_number")
    business_99_enabled = _macrolead_has_columns("business_99_status")
    lead_created_at_enabled = _macrolead_has_columns("lead_created_at")
    store_id_enabled = _macrolead_has_columns("store_id")
    signatory_id_enabled = _macrolead_has_columns("signatory_id")
    selected_ddds = _parse_ddd_filter(ddd_filter)
    duplicate_store_ids = []
    if store_id_enabled:
        duplicate_store_ids = (
            _base_macrolead_queryset()
            .exclude(store_id="")
            .values("store_id")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .values_list("store_id", flat=True)
        )

    if q:
        text_filter = (
            Q(city__icontains=q)
            | Q(target_region__icontains=q)
            | Q(establishment_name__icontains=q)
            | Q(representative_name__icontains=q)
            | Q(contract_status__icontains=q)
            | Q(representative_phone__icontains=q)
            | Q(company_category__icontains=q)
            | Q(address__icontains=q)
        )
        if business_99_enabled:
            text_filter |= Q(business_99_status__icontains=q)
        if store_id_enabled:
            text_filter |= Q(store_id__icontains=q)
        if signatory_id_enabled:
            text_filter |= Q(signatory_id__icontains=q)
        if q_digits:
            if phone_norm_enabled:
                text_filter |= Q(representative_phone_norm__icontains=q_digits)
            else:
                text_filter |= Q(representative_phone__icontains=q_digits)
        queryset = queryset.filter(text_filter)
    if city:
        queryset = queryset.filter(city=city)
    if selected_ddds:
        ddd_query = Q()
        for ddd in selected_ddds:
            if phone_norm_enabled:
                ddd_query |= Q(representative_phone_norm__startswith=f"55{ddd}")
                ddd_query |= Q(representative_phone_norm__startswith=ddd)
            ddd_query |= (
                Q(representative_phone__icontains=f"({ddd})")
                | Q(representative_phone__startswith=ddd)
                | Q(representative_phone__icontains=f" {ddd} ")
                | Q(representative_phone__icontains=f"+55{ddd}")
                | Q(representative_phone__icontains=f"55{ddd}")
            )
        queryset = queryset.filter(ddd_query)
    if contract_status:
        queryset = queryset.filter(contract_status=contract_status)
    if business_99_enabled and business_99_status:
        queryset = queryset.filter(business_99_status=business_99_status)
    if company_category:
        queryset = queryset.filter(company_category=company_category)
    if representative_presence == "with":
        queryset = queryset.exclude(_missing_representative_q())
    elif representative_presence == "without":
        queryset = queryset.filter(_missing_representative_q())
    if blocked_enabled:
        if blocked == "yes":
            queryset = queryset.filter(is_blocked_number=True)
        elif blocked == "no":
            queryset = queryset.filter(is_blocked_number=False)
    if store_id_enabled:
        if phone_dup == "duplicates":
            queryset = queryset.filter(store_id__in=duplicate_store_ids)
        elif phone_dup == "unique":
            queryset = queryset.exclude(store_id="").exclude(
                store_id__in=duplicate_store_ids
            )
        elif phone_dup == "empty":
            queryset = queryset.filter(store_id="")
    if _export_tracking_enabled():
        if export_status == "exported":
            queryset = queryset.exclude(exported_at__isnull=True)
        elif export_status == "not_exported":
            queryset = queryset.filter(exported_at__isnull=True)
    if lead_created_at_enabled and lead_date_from:
        queryset = queryset.filter(lead_created_at__date__gte=lead_date_from)
    if lead_created_at_enabled and lead_date_to:
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
            "ddd_filter",
            "city",
            "contract_status",
            "business_99_status",
            "company_category",
            "representative_presence",
            "blocked",
            "phone_dup",
            "export_status",
            "lead_date_from",
            "lead_date_to",
        )
    }
    cleaned = {k: v for k, v in cleaned.items() if v}
    if not cleaned:
        return redirect("macro_list")
    return redirect(f"{base}?{urlencode(cleaned)}")


def _lead_sources():
    if not _macrolead_table_ready():
        return []
    return (
        _base_macrolead_queryset().exclude(source="")
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


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _close_stale_running_runs() -> int:
    if not _macrorun_table_ready():
        return 0
    stale_minutes = max(1, int(getattr(settings, "MACRO_RUN_STALE_MINUTES", 30) or 30))
    cutoff = timezone.now() - timedelta(minutes=stale_minutes)
    stale_qs = _base_macrorun_queryset().filter(status="running", started_at__lt=cutoff, finished_at__isnull=True)
    stale_count = stale_qs.count()
    if stale_count:
        stale_qs.update(
            status="error",
            finished_at=timezone.now(),
            message=f"Execucao encerrada automaticamente apos {stale_minutes} minutos sem finalizacao.",
        )
    return stale_count


def _parse_export_fields(params) -> list[str]:
    raw_values = []
    if hasattr(params, "getlist"):
        raw_values.extend(params.getlist("export_fields"))
    single = (params.get("export_fields") or "").strip() if hasattr(params, "get") else ""
    if single and not raw_values:
        raw_values.append(single)

    selected = []
    allowed = set(_available_export_field_labels().keys())
    for raw in raw_values:
        for token in str(raw).split(","):
            field = token.strip()
            if not field or field not in allowed or field in selected:
                continue
            selected.append(field)

    if not selected:
        return list(_default_export_fields())
    return selected


def _parse_export_limit(params) -> int | None:
    raw = (params.get("export_limit") or "").strip() if hasattr(params, "get") else ""
    if not raw:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return min(value, MAX_EXPORT_LIMIT)


def _parse_mark_exported(params) -> bool:
    raw = (params.get("mark_exported") or "").strip().lower() if hasattr(params, "get") else ""
    return raw in {"1", "true", "yes", "on"}


def _export_cell_value(item: MacroLead, field: str):
    value = getattr(item, field, "")
    if field in {"lead_created_at", "first_seen_at", "last_seen_at", "exported_at"} and value:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _city_report_queryset(params=None):
    params = params or {}
    if not _macrolead_table_ready():
        return []

    city_qs = _base_macrolead_queryset().exclude(city="")
    city_contains = (params.get("city_contains") or "").strip()
    min_count = _safe_int(params.get("min_count"), 0)
    max_count = _safe_int(params.get("max_count"), 0)

    if city_contains:
        city_qs = city_qs.filter(city__icontains=city_contains)

    grouped = city_qs.values("city").annotate(total=Count("id")).order_by("-total", "city")

    if min_count:
        grouped = grouped.filter(total__gte=min_count)
    if max_count:
        grouped = grouped.filter(total__lte=max_count)

    return grouped


def _empty_macro_stats():
    return {
        "total_records": 0,
        "total_cities": 0,
        "phones_filled": 0,
        "phones_unique": 0,
        "blocked_numbers": 0,
        "blocked_numbers_unique": 0,
        "total_categories": 0,
        "duplicate_store_ids": 0,
        "duplicate_store_leads": 0,
    }


@login_required
@user_passes_test(_staff_access)
def macro_list(request):
    stale_count = _close_stale_running_runs()
    if stale_count:
        messages.warning(request, f"{stale_count} execucao(oes) antiga(s) em andamento foram encerradas automaticamente.")
    version_meta = _macro_agent_version_meta()
    lead_table_ready = _macrolead_table_ready()
    run_table_ready = _macrorun_table_ready()
    phone_norm_enabled = _macrolead_has_columns("representative_phone_norm")
    blocked_enabled = _macrolead_has_columns("is_blocked_number")
    lead_created_at_enabled = _macrolead_has_columns("lead_created_at")
    business_99_enabled = _macrolead_has_columns("business_99_status")
    export_tracking_enabled = _export_tracking_enabled()
    store_id_enabled = _macrolead_has_columns("store_id")
    signatory_id_enabled = _macrolead_has_columns("signatory_id")
    run_pages_enabled = _macrorun_has_columns("pages_processed")
    if not lead_table_ready:
        messages.warning(
            request,
            "Tabela principal da Macro indisponivel no banco. Aplique as migrations pendentes e recarregue a pagina.",
        )
    if not run_table_ready:
        messages.warning(
            request,
            "Tabela de historico da Macro indisponivel no banco. O Banco Macro abrira, mas sem historico ate aplicar as migrations.",
        )
    if lead_table_ready and not export_tracking_enabled:
        messages.warning(
            request,
            "Campos de exportacao ainda nao existem no banco. Aplique a migration 0010 para ativar marcacao de exportados.",
        )
    if lead_table_ready and (not store_id_enabled or not signatory_id_enabled):
        messages.warning(
            request,
            "Campos de ID ainda nao existem no banco. Aplique a migration 0009 para ativar ID da loja e ID do signatario.",
        )
    if lead_table_ready:
        all_queryset = _base_macrolead_queryset()
        filtered_queryset = _apply_filters(request, queryset=all_queryset)
        paginator = Paginator(filtered_queryset, 100)
        page_obj = paginator.get_page(request.GET.get("page"))
    else:
        all_queryset = None
        filtered_queryset = None
        page_obj = Paginator([], 100).get_page(request.GET.get("page"))
    query_params = request.GET.copy()
    query_params.pop("page", None)

    stats = _empty_macro_stats()
    if lead_table_ready:
        phone_field = "representative_phone_norm" if phone_norm_enabled else "representative_phone"
        aggregate_kwargs = {
            "total_records": Count("id"),
            "total_cities": Count("city", distinct=True, filter=~Q(city="")),
            "phones_filled": Count("id", filter=~Q(**{phone_field: ""})),
            "phones_unique": Count(phone_field, distinct=True, filter=~Q(**{phone_field: ""})),
            "total_categories": Count("company_category", distinct=True, filter=~Q(company_category="")),
        }
        if blocked_enabled:
            aggregate_kwargs["blocked_numbers"] = Count("id", filter=Q(is_blocked_number=True))
            aggregate_kwargs["blocked_numbers_unique"] = Count(
                phone_field,
                distinct=True,
                filter=Q(is_blocked_number=True) & ~Q(**{phone_field: ""}),
            )
        stats.update(all_queryset.aggregate(**aggregate_kwargs))
        if store_id_enabled:
            duplicate_store_stats = (
                all_queryset.exclude(store_id="")
                .values("store_id")
                .annotate(total=Count("id"))
                .filter(total__gt=1)
            )
            stats["duplicate_store_ids"] = duplicate_store_stats.count()
            stats["duplicate_store_leads"] = sum(row["total"] for row in duplicate_store_stats)
    else:
        stats["duplicate_store_ids"] = 0
        stats["duplicate_store_leads"] = 0

    page_store_ids = {
        (item.store_id or "").strip()
        for item in page_obj.object_list
        if lead_table_ready and store_id_enabled and (item.store_id or "").strip()
    }
    page_duplicate_store_ids = set()
    if lead_table_ready and store_id_enabled and page_store_ids:
        page_duplicate_store_ids = set(
            all_queryset.filter(store_id__in=page_store_ids)
            .values("store_id")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .values_list("store_id", flat=True)
        )
    if lead_table_ready:
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
        if business_99_enabled:
            business_99_breakdown = (
                all_queryset.exclude(business_99_status="")
                .values("business_99_status")
                .annotate(total=Count("id"))
                .order_by("-total", "business_99_status")[:10]
            )
            business_99_statuses = (
                _base_macrolead_queryset().exclude(business_99_status="")
                .values_list("business_99_status", flat=True)
                .distinct()
                .order_by("business_99_status")
            )
        else:
            business_99_breakdown = []
            business_99_statuses = []
        last_capture_at = all_queryset.order_by("-last_seen_at").values_list("last_seen_at", flat=True).first()
        filtered_count = filtered_queryset.count()
        cities = _base_macrolead_queryset().exclude(city="").values_list("city", flat=True).distinct().order_by("city")
        contract_statuses = (
            _base_macrolead_queryset().exclude(contract_status="")
            .values_list("contract_status", flat=True)
            .distinct()
            .order_by("contract_status")
        )
        categories = (
            _base_macrolead_queryset().exclude(company_category="")
            .values_list("company_category", flat=True)
            .distinct()
            .order_by("company_category")
        )
    else:
        city_breakdown = []
        category_breakdown = []
        status_breakdown = []
        business_99_breakdown = []
        business_99_statuses = []
        last_capture_at = None
        filtered_count = 0
        cities = []
        contract_statuses = []
        categories = []

    if run_table_ready:
        recent_runs = _base_macrorun_queryset()[:12]
        last_success_run = _base_macrorun_queryset().filter(status="success").first()
    else:
        recent_runs = []
        last_success_run = None
    selected_lead_date_from, selected_lead_date_to = _coerce_date_range(
        (request.GET.get("lead_date_from") or "").strip(),
        (request.GET.get("lead_date_to") or "").strip(),
    )

    context = {
        "active_tab": "database",
        "page_obj": page_obj,
        "filtered_count": filtered_count,
        "cities": cities,
        "contract_statuses": contract_statuses,
        "business_99_statuses": business_99_statuses,
        "categories": categories,
        "selected_ddd_filter": (request.GET.get("ddd_filter") or "").strip(),
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
        "page_duplicate_store_ids": page_duplicate_store_ids,
        "last_capture_at": last_capture_at,
        "last_success_run": last_success_run,
        "city_breakdown": city_breakdown,
        "category_breakdown": category_breakdown,
        "status_breakdown": status_breakdown,
        "business_99_breakdown": business_99_breakdown,
        "sources": _lead_sources(),
        "export_field_choices": _available_export_field_choices(),
        "default_export_fields": _default_export_fields(),
        "max_export_limit": MAX_EXPORT_LIMIT,
        "selected_export_fields": _parse_export_fields(request.GET),
        "selected_export_limit": request.GET.get("export_limit", "").strip(),
        "selected_export_status": (request.GET.get("export_status") or "").strip().lower(),
        "selected_mark_exported": (_parse_mark_exported(request.GET) if "mark_exported" in request.GET else True) and export_tracking_enabled,
        "selected_lead_date_from": selected_lead_date_from,
        "selected_lead_date_to": selected_lead_date_to,
        "export_tracking_enabled": export_tracking_enabled,
        "lead_table_ready": lead_table_ready,
        "run_table_ready": run_table_ready,
        "phone_norm_enabled": phone_norm_enabled,
        "blocked_enabled": blocked_enabled,
        "lead_created_at_enabled": lead_created_at_enabled,
        "business_99_enabled": business_99_enabled,
        "run_pages_enabled": run_pages_enabled,
        "store_id_enabled": store_id_enabled,
        "signatory_id_enabled": signatory_id_enabled,
        "results_colspan": 10 + (1 if store_id_enabled else 0) + (1 if signatory_id_enabled else 0) + (1 if business_99_enabled else 0) + (1 if lead_created_at_enabled else 0) + (1 if export_tracking_enabled else 0),
    }
    return render(request, "macros/list.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_collect(request):
    stale_count = _close_stale_running_runs()
    if stale_count:
        messages.warning(request, f"{stale_count} execucao(oes) antiga(s) em andamento foram encerradas automaticamente.")
    version_meta = _macro_agent_version_meta()
    lead_table_ready = _macrolead_table_ready()
    run_table_ready = _macrorun_table_ready()
    run_pages_enabled = _macrorun_has_columns("pages_processed")
    if not lead_table_ready:
        messages.warning(
            request,
            "Tabela principal da Macro indisponivel no banco. Aplique as migrations pendentes para restaurar a coleta completa.",
        )
    if not run_table_ready:
        messages.warning(
            request,
            "Tabela de historico da Macro indisponivel no banco. A coleta local abrira sem o historico ate aplicar as migrations.",
        )
    if lead_table_ready and not _export_tracking_enabled():
        messages.warning(
            request,
            "Campos de exportacao ainda nao existem no banco. Aplique a migration 0010 para ativar marcacao de exportados.",
        )
    last_success_run = _base_macrorun_queryset().filter(status="success").first() if run_table_ready else None
    last_error_run = _base_macrorun_queryset().filter(status="error").first() if run_table_ready else None
    runs_24h = _base_macrorun_queryset().filter(started_at__gte=timezone.now() - timedelta(hours=24)) if run_table_ready else []
    exe_path = Path(settings.MACRO_LOCAL_AGENT_EXE_PATH)
    context = {
        "active_tab": "collect",
        "api_import_url": request.build_absolute_uri(reverse("macro_api_import")),
        "macro_target_url": settings.MACRO_TARGET_URL,
        "token_configured": bool(settings.MACRO_API_TOKEN),
        "recent_runs": _base_macrorun_queryset()[:12] if run_table_ready else [],
        "local_agent_url": settings.MACRO_LOCAL_AGENT_URL,
        "local_agent_exe_available": exe_path.exists(),
        "macro_agent_version": version_meta["version"],
        "macro_agent_build": version_meta["build"],
        "macro_agent_label": version_meta["label"],
        "total_records": _base_macrolead_queryset().count() if lead_table_ready else 0,
        "last_capture_at": _base_macrolead_queryset().order_by("-last_seen_at").values_list("last_seen_at", flat=True).first() if lead_table_ready else None,
        "last_success_run": last_success_run,
        "last_error_run": last_error_run,
        "runs_24h_total": runs_24h.count() if run_table_ready else 0,
        "runs_24h_error": runs_24h.filter(status="error").count() if run_table_ready else 0,
        "sources": _lead_sources(),
        "run_pages_enabled": run_pages_enabled,
    }
    return render(request, "macros/collect.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_export_csv(request):
    queryset = _apply_filters(request)
    export_limit = _parse_export_limit(request.GET)
    if export_limit:
        queryset = queryset[:export_limit]
    selected_fields = _parse_export_fields(request.GET)
    mark_exported = _parse_mark_exported(request.GET)
    rows = list(queryset)
    if mark_exported and rows and _export_tracking_enabled():
        now = timezone.now()
        batch_id = str(uuid.uuid4())
        ids = [item.id for item in rows]
        MacroLead.objects.filter(id__in=ids).update(exported_at=now, export_batch_id=batch_id)
        for item in rows:
            item.exported_at = now
            item.export_batch_id = batch_id

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="macro_leads.csv"'

    writer = csv.writer(response)
    export_labels = _available_export_field_labels()
    writer.writerow([export_labels[field] for field in selected_fields])
    for item in rows:
        writer.writerow([_export_cell_value(item, field) for field in selected_fields])
    return response


@login_required
@user_passes_test(_staff_access)
def macro_export_xlsx(request):
    from openpyxl import Workbook

    queryset = _apply_filters(request)
    export_limit = _parse_export_limit(request.GET)
    if export_limit:
        queryset = queryset[:export_limit]
    selected_fields = _parse_export_fields(request.GET)
    mark_exported = _parse_mark_exported(request.GET)
    rows = list(queryset)
    if mark_exported and rows and _export_tracking_enabled():
        now = timezone.now()
        batch_id = str(uuid.uuid4())
        ids = [item.id for item in rows]
        MacroLead.objects.filter(id__in=ids).update(exported_at=now, export_batch_id=batch_id)
        for item in rows:
            item.exported_at = now
            item.export_batch_id = batch_id

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="macro_leads.xlsx"'

    wb = Workbook()
    ws = wb.active
    ws.title = "Macro Leads"
    export_labels = _available_export_field_labels()
    ws.append([export_labels[field] for field in selected_fields])
    for item in rows:
        ws.append([_export_cell_value(item, field) for field in selected_fields])
    wb.save(response)
    return response


@login_required
@user_passes_test(_staff_access)
def macro_city_report(request):
    grouped = _city_report_queryset(request.GET)
    city_count = len(grouped) if isinstance(grouped, list) else grouped.count()
    context = {
        "active_tab": "database",
        "macro_agent_label": _macro_agent_version_meta()["label"],
        "city_rows": grouped,
        "city_total": city_count,
        "city_contains": (request.GET.get("city_contains") or "").strip(),
        "min_count": (request.GET.get("min_count") or "").strip(),
        "max_count": (request.GET.get("max_count") or "").strip(),
    }
    return render(request, "macros/city_report.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_city_report_csv(request):
    grouped = _city_report_queryset(request.GET)
    if not isinstance(grouped, list):
        grouped = list(grouped[:MAX_CITY_REPORT_EXPORT])

    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="macro_cidades.csv"'
    writer = csv.writer(response)
    writer.writerow(["Cidade", "Quantidade"])
    for row in grouped:
        writer.writerow([row["city"], row["total"]])
    return response


@login_required
@user_passes_test(_staff_access)
def macro_city_report_xlsx(request):
    from openpyxl import Workbook

    grouped = _city_report_queryset(request.GET)
    if not isinstance(grouped, list):
        grouped = list(grouped[:MAX_CITY_REPORT_EXPORT])

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="macro_cidades.xlsx"'

    wb = Workbook()
    ws = wb.active
    ws.title = "Cidades"
    ws.append(["Cidade", "Quantidade"])
    for row in grouped:
        ws.append([row["city"], row["total"]])
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
set -euo pipefail
cd "$(dirname "$0")"

STAMP_FILE=".venv/.deps_ok_v1"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 nao encontrado."
  echo "Instale com: xcode-select --install"
  echo "Ou via Homebrew: brew install python"
  exit 1
fi

if ! python3 --version >/dev/null 2>&1; then
  echo "python3 encontrado, mas ainda nao funcional."
  echo "Conclua a instalacao das Command Line Tools com: xcode-select --install"
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo "[mac-agent] Criando ambiente virtual (.venv)..."
  python3 -m venv .venv
fi

source .venv/bin/activate

if [ ! -f "$STAMP_FILE" ]; then
  echo "[mac-agent] Instalando dependencias (primeira execucao)..."
  python -m pip install --upgrade pip
  python -m pip install selenium webdriver-manager requests
  touch "$STAMP_FILE"
fi

if ! python - <<'PY'
import importlib.util
mods = ("selenium", "webdriver_manager", "requests")
missing = [m for m in mods if importlib.util.find_spec(m) is None]
raise SystemExit(0 if not missing else 1)
PY
then
  echo "[mac-agent] Dependencias ausentes. Reinstalando..."
  python -m pip install --upgrade pip
  python -m pip install selenium webdriver-manager requests
  touch "$STAMP_FILE"
fi

echo "[mac-agent] Iniciando painel local em http://127.0.0.1:8765/"
python local_macro_agent.py
"""
    readme = """Coletor Local para macOS

1) Extraia este .zip em uma pasta.
2) No Finder, clique com o botao direito em iniciar_coletor_mac.command e escolha Abrir.
3) Se o macOS bloquear, va em Ajustes > Privacidade e Seguranca > Abrir Mesmo Assim.
4) Na primeira execucao ele prepara ambiente e instala dependencias automaticamente.
5) Depois disso, use apenas duplo clique no iniciar_coletor_mac.command.
6) O coletor abrira em http://127.0.0.1:8765
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

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "detail": "Invalid JSON"}, status=400)

    meta = payload.get("meta") if isinstance(payload, dict) and isinstance(payload.get("meta"), dict) else {}
    execution_id = str(meta.get("execution_id") or "").strip()
    batch_index = _safe_int(meta.get("batch_index"), 0)
    batch_total = _safe_int(meta.get("batch_total"), 0)
    pages_processed = _safe_int(meta.get("pages_processed"), 0)
    collected_total = _safe_int(meta.get("collected_total"), 0)
    deduplicated_total = _safe_int(meta.get("deduplicated_total"), 0)
    sent_after = _safe_int(meta.get("sent_after"), 0)
    client_ip = _client_ip(request) or None

    _close_stale_running_runs()

    run_log = None
    if execution_id:
        run_log = (
            MacroRun.objects.filter(run_type="api", execution_id=execution_id)
            .order_by("-started_at")
            .first()
        )
    if run_log is None:
        run_log = MacroRun.objects.create(
            run_type="api",
            status="running",
            source="api",
            execution_id=execution_id,
            triggered_by=request.user if request.user.is_authenticated else None,
            request_ip=client_ip,
        )
    elif run_log.status != "running":
        run_log.status = "running"
        run_log.finished_at = None
        run_log.save(update_fields=["status", "finished_at"])

    rows = _rows_from_json_payload(payload)
    if not rows:
        run_log.status = "error"
        run_log.pages_processed = max(run_log.pages_processed, pages_processed)
        run_log.total_collected = max(run_log.total_collected, collected_total)
        run_log.total_deduplicated = max(run_log.total_deduplicated, deduplicated_total)
        run_log.total_sent = max(run_log.total_sent, sent_after)
        if client_ip:
            run_log.request_ip = client_ip
        run_log.message = "Payload sem linhas."
        run_log.finished_at = timezone.now()
        run_log.save(
            update_fields=[
                "status",
                "message",
                "finished_at",
                "pages_processed",
                "total_collected",
                "total_deduplicated",
                "total_sent",
                "request_ip",
            ]
        )
        return JsonResponse({"ok": False, "detail": "Payload sem linhas"}, status=400)

    try:
        result = upsert_rows(rows, default_source="api")
    except Exception:
        logger.exception("Falha interna no import da macro API")
        run_log.status = "error"
        run_log.finished_at = timezone.now()
        run_log.total_collected = max(run_log.total_collected, collected_total, len(rows))
        run_log.total_deduplicated = max(run_log.total_deduplicated, deduplicated_total, len(rows))
        run_log.pages_processed = max(run_log.pages_processed, pages_processed)
        run_log.total_sent = max(run_log.total_sent, sent_after)
        if client_ip:
            run_log.request_ip = client_ip
        run_log.message = "Erro interno ao processar lote da API."
        run_log.save(
            update_fields=[
                "status",
                "finished_at",
                "total_collected",
                "total_deduplicated",
                "pages_processed",
                "total_sent",
                "request_ip",
                "message",
            ]
        )
        return JsonResponse({"ok": False, "detail": "Internal processing error"}, status=500)

    final_batch = batch_total <= 1 or (batch_index > 0 and batch_index >= batch_total)
    message_parts = []
    if final_batch:
        message_parts.append("Importacao API concluida.")
    else:
        message_parts.append("Importacao API em andamento.")
    if batch_total > 1 and batch_index > 0:
        message_parts.append(f"Lote {batch_index}/{batch_total}.")
    if pages_processed > 0:
        message_parts.append(f"Paginas: {pages_processed}.")
    if collected_total > 0:
        message_parts.append(f"Coletados brutos: {collected_total}.")
    if deduplicated_total > 0:
        message_parts.append(f"Linhas enviadas: {deduplicated_total}.")

    if final_batch:
        run_log.status = "success"
        run_log.finished_at = timezone.now()
    else:
        run_log.status = "running"
        run_log.finished_at = None
    run_log.total_collected = max(run_log.total_collected, collected_total, result["processed"])
    run_log.total_deduplicated = max(run_log.total_deduplicated, deduplicated_total, result["processed"])
    if sent_after > 0:
        run_log.total_received = max(run_log.total_received, sent_after)
        run_log.total_sent = max(run_log.total_sent, sent_after)
    else:
        run_log.total_received = max(run_log.total_received, result["processed"])
    run_log.pages_processed = max(run_log.pages_processed, pages_processed)
    run_log.created_count = run_log.created_count + result["created"]
    run_log.updated_count = run_log.updated_count + result["updated"]
    run_log.ignored_count = run_log.ignored_count + result["ignored"]
    run_log.invalid_count = run_log.invalid_count + result["invalid"]
    if client_ip:
        run_log.request_ip = client_ip
    run_log.message = " ".join(message_parts)
    run_log.save(
        update_fields=[
            "status",
            "finished_at",
            "request_ip",
            "total_collected",
            "total_deduplicated",
            "total_received",
            "total_sent",
            "pages_processed",
            "created_count",
            "updated_count",
            "ignored_count",
            "invalid_count",
            "message",
        ]
    )
    return JsonResponse({"ok": True, **result})
