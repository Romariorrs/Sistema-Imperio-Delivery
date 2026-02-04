import csv
import io
import json
import secrets
from pathlib import Path
from typing import Iterable

from django.conf import settings
from django.core.cache import cache
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from .models import MacroLead, MacroRun
from .services import EXPORT_COLUMNS, upsert_rows


def _staff_access(user):
    return user.is_staff or user.is_superuser


def _apply_filters(request, queryset=None):
    queryset = queryset or MacroLead.objects.all()
    q = (request.GET.get("q") or "").strip()
    q_digits = "".join(char for char in q if char.isdigit())
    city = (request.GET.get("city") or "").strip()
    contract_status = (request.GET.get("contract_status") or "").strip()
    company_category = (request.GET.get("company_category") or "").strip()

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
        if q_digits:
            text_filter |= Q(representative_phone_norm__icontains=q_digits)
        queryset = queryset.filter(text_filter)
    if city:
        queryset = queryset.filter(city=city)
    if contract_status:
        queryset = queryset.filter(contract_status=contract_status)
    if company_category:
        queryset = queryset.filter(company_category=company_category)
    return queryset.order_by("-last_seen_at", "-id")


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
    queryset = _apply_filters(request)
    paginator = Paginator(queryset, 100)
    page_obj = paginator.get_page(request.GET.get("page"))
    query_params = request.GET.copy()
    query_params.pop("page", None)

    context = {
        "page_obj": page_obj,
        "total_count": queryset.count(),
        "cities": MacroLead.objects.exclude(city="").values_list("city", flat=True).distinct().order_by("city"),
        "contract_statuses": MacroLead.objects.exclude(contract_status="")
        .values_list("contract_status", flat=True)
        .distinct()
        .order_by("contract_status"),
        "categories": MacroLead.objects.exclude(company_category="")
        .values_list("company_category", flat=True)
        .distinct()
        .order_by("company_category"),
        "api_import_url": request.build_absolute_uri(reverse("macro_api_import")),
        "macro_target_url": settings.MACRO_TARGET_URL,
        "token_configured": bool(settings.MACRO_API_TOKEN),
        "filter_querystring": query_params.urlencode(),
        "recent_runs": MacroRun.objects.all()[:12],
        "local_agent_url": settings.MACRO_LOCAL_AGENT_URL,
    }
    return render(request, "macros/list.html", context)


@login_required
@user_passes_test(_staff_access)
def macro_export_csv(request):
    queryset = _apply_filters(request)
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="macro_leads.csv"'

    writer = csv.writer(response)
    writer.writerow([label for _, label in EXPORT_COLUMNS] + ["Source", "Primeira captura", "Ultima captura"])
    for item in queryset:
        writer.writerow(
            [getattr(item, field, "") for field, _ in EXPORT_COLUMNS]
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
        ws.append(
            [getattr(item, field, "") for field, _ in EXPORT_COLUMNS]
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

    result = upsert_rows(rows, default_source="api")
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
