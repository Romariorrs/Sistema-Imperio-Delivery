from django.contrib import admin

from .models import MacroExportLog, MacroLead, MacroRun


@admin.register(MacroLead)
class MacroLeadAdmin(admin.ModelAdmin):
    list_display = (
        "store_id",
        "signatory_id",
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
        "store_id",
        "signatory_id",
        "establishment_name",
        "representative_name",
        "representative_phone",
        "representative_phone_norm",
        "address",
    )


@admin.register(MacroExportLog)
class MacroExportLogAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "title",
        "user",
        "channel",
        "lead_count",
        "marked_as_exported",
    )
    list_filter = ("channel", "marked_as_exported")
    search_fields = ("title", "user__username", "user__email", "filter_querystring")
    readonly_fields = ("created_at",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


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
        "ignored_count",
    )
    list_filter = ("run_type", "status", "source")
    search_fields = ("message", "request_ip")
    readonly_fields = ("started_at", "finished_at")
