from django.contrib import admin

from .models import MacroLead, MacroRun


@admin.register(MacroLead)
class MacroLeadAdmin(admin.ModelAdmin):
    list_display = (
        "establishment_name",
        "city",
        "target_region",
        "contract_status",
        "representative_phone",
        "is_blocked_number",
        "last_seen_at",
    )
    list_filter = ("city", "contract_status", "company_category", "source", "is_blocked_number")
    search_fields = (
        "establishment_name",
        "representative_name",
        "representative_phone",
        "representative_phone_norm",
        "address",
    )


@admin.register(MacroRun)
class MacroRunAdmin(admin.ModelAdmin):
    list_display = (
        "started_at",
        "run_type",
        "status",
        "source",
        "total_collected",
        "total_sent",
        "created_count",
        "updated_count",
    )
    list_filter = ("run_type", "status", "source")
    search_fields = ("message", "request_ip")
    readonly_fields = ("started_at", "finished_at")
