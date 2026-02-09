import hashlib
import re
import unicodedata
from datetime import datetime, timezone as dt_timezone, timedelta
from typing import Any, Dict, Iterable, Mapping, Optional

from django.db import IntegrityError
from django.utils import timezone
from django.utils.dateparse import parse_datetime, parse_date

from .models import MacroLead

EXPORT_COLUMNS = (
    ("city", "Cidade"),
    ("target_region", "Regiao-alvo"),
    ("lead_created_at", "Horario de criacao do lead"),
    ("establishment_name", "Nome do estabelecimento"),
    ("representative_name", "Nome do representante 99"),
    ("contract_status", "Status do contrato"),
    ("representative_phone", "Telefone do representante do estabelecimento"),
    ("company_category", "Categoria da empresa"),
    ("address", "Endereco"),
)

HEADER_ALIASES = {
    "cidade": "city",
    "city": "city",
    "regiao alvo": "target_region",
    "regiao-alvo": "target_region",
    "target region": "target_region",
    "target_region": "target_region",
    "horario de criacao do lead": "lead_created_at",
    "horario criacao do lead": "lead_created_at",
    "lead created at": "lead_created_at",
    "lead_created_at": "lead_created_at",
    "nome do estabelecimento": "establishment_name",
    "establishment_name": "establishment_name",
    "nome do representante 99": "representative_name",
    "representative_name": "representative_name",
    "status do contrato": "contract_status",
    "contract_status": "contract_status",
    "telefone do representante do estabelecimento": "representative_phone",
    "representative_phone": "representative_phone",
    "categoria da empresa": "company_category",
    "company_category": "company_category",
    "endereco": "address",
    "address": "address",
    "source": "source",
}


def normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^\w\s-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_header(value: Any) -> str:
    return normalize_text(value).replace("_", " ").lower()


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if digits.startswith("55") and len(digits) > 11:
        return digits
    if len(digits) in (10, 11):
        return "55" + digits
    return digits


def normalize_value(field: str, value: Any) -> str:
    text = str(value or "").strip()
    if field in {"city", "target_region", "contract_status", "company_category"}:
        return normalize_text(text)
    if field in {"establishment_name", "representative_name", "address"}:
        return re.sub(r"\s+", " ", text).strip()
    if field == "representative_phone":
        return text
    return text


def parse_lead_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None

    # Try standard parsers first
    dt = parse_datetime(raw)
    if dt:
        if timezone.is_naive(dt):
            return timezone.make_aware(dt)
        return dt

    # Common formats like "2026-02-02 13:45:20 UTC-3"
    match = re.search(
        r"(?P<date>\d{4}-\d{2}-\d{2})[ T](?P<time>\d{2}:\d{2}(?::\d{2})?)"
        r"(?:\s*UTC(?P<offset>[+-]\d{1,2})(?::?(?P<mins>\d{2}))?)?",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        date_part = match.group("date")
        time_part = match.group("time")
        try:
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M")
        offset = match.group("offset")
        mins = match.group("mins")
        if offset:
            hours = int(offset)
            minutes = int(mins) if mins else 0
            tz = dt_timezone(timedelta(hours=hours, minutes=minutes))
            return dt.replace(tzinfo=tz).astimezone(timezone.get_current_timezone())
        return timezone.make_aware(dt)

    # Fallback: date only
    d = parse_date(raw)
    if d:
        return timezone.make_aware(datetime(d.year, d.month, d.day))
    return None


def normalize_row(raw_row: Mapping[str, Any], default_source: str = "gattaran") -> Dict[str, str]:
    parsed: Dict[str, Any] = {field: "" for field, _ in EXPORT_COLUMNS}
    parsed["lead_created_at"] = None
    parsed["source"] = default_source
    parsed["representative_phone_norm"] = ""

    for key, value in raw_row.items():
        mapped = HEADER_ALIASES.get(normalize_header(key))
        if not mapped:
            continue
        if mapped == "lead_created_at":
            parsed[mapped] = parse_lead_datetime(value)
        else:
            parsed[mapped] = normalize_value(mapped, value)

    parsed["representative_phone_norm"] = normalize_phone(parsed.get("representative_phone", ""))

    return parsed


def build_unique_key(parsed_row: Mapping[str, Any]) -> str:
    stable_parts = [
        normalize_header(parsed_row.get("source", "")),
        normalize_header(parsed_row.get("city", "")),
        normalize_header(parsed_row.get("establishment_name", "")),
        normalize_header(parsed_row.get("representative_phone_norm", "")),
    ]
    if not stable_parts[-1]:
        stable_parts.append(normalize_header(parsed_row.get("address", "")))
        stable_parts.append(normalize_header(parsed_row.get("representative_name", "")))
    parts = stable_parts
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def upsert_rows(rows: Iterable[Mapping[str, Any]], default_source: str = "gattaran") -> Dict[str, int]:
    created = 0
    updated = 0
    ignored = 0
    invalid = 0

    for raw in rows:
        if not isinstance(raw, Mapping):
            invalid += 1
            continue

        parsed = normalize_row(raw, default_source=default_source)
        if not any(parsed.get(field) for field, _ in EXPORT_COLUMNS):
            ignored += 1
            continue

        parsed["unique_key"] = build_unique_key(parsed)
        defaults = {**parsed}
        unique_key = defaults.pop("unique_key")
        lead = MacroLead.objects.filter(unique_key=unique_key).first()
        was_created = False
        if not lead:
            lead = (
                MacroLead.objects.filter(
                    source=parsed["source"],
                    city__iexact=parsed["city"],
                    establishment_name__iexact=parsed["establishment_name"],
                    representative_phone_norm=parsed["representative_phone_norm"],
                )
                .order_by("-last_seen_at")
                .first()
            )
        if not lead and parsed["address"]:
            lead = (
                MacroLead.objects.filter(
                    source=parsed["source"],
                    city__iexact=parsed["city"],
                    establishment_name__iexact=parsed["establishment_name"],
                    address__iexact=parsed["address"],
                )
                .order_by("-last_seen_at")
                .first()
            )
        if not lead:
            blocked_phone = defaults.get("representative_phone_norm", "")
            is_blocked = False
            if blocked_phone:
                is_blocked = MacroLead.objects.filter(
                    representative_phone_norm=blocked_phone,
                    is_blocked_number=True,
                ).exists()
            lead = MacroLead.objects.create(
                unique_key=unique_key,
                is_blocked_number=is_blocked,
                **defaults,
            )
            was_created = True

        if was_created:
            created += 1
            continue

        changed_fields = []
        if lead.unique_key != unique_key:
            lead.unique_key = unique_key
            changed_fields.append("unique_key")
        for field in defaults:
            incoming = defaults[field]
            if getattr(lead, field) != incoming:
                setattr(lead, field, incoming)
                changed_fields.append(field)

        current_phone = lead.representative_phone_norm
        if current_phone and not lead.is_blocked_number:
            if MacroLead.objects.filter(
                representative_phone_norm=current_phone,
                is_blocked_number=True,
            ).exclude(id=lead.id).exists():
                lead.is_blocked_number = True
                changed_fields.append("is_blocked_number")

        lead.last_seen_at = timezone.now()
        changed_fields.append("last_seen_at")
        try:
            lead.save(update_fields=changed_fields)
        except IntegrityError:
            if "unique_key" in changed_fields:
                lead.unique_key = f"{unique_key[:56]}{lead.id:08d}"[:64]
                safe_fields = [field for field in changed_fields if field != "unique_key"] + ["unique_key"]
                lead.save(update_fields=safe_fields)
            else:
                raise
        updated += 1

    return {
        "created": created,
        "updated": updated,
        "ignored": ignored,
        "invalid": invalid,
        "processed": created + updated + ignored + invalid,
    }
