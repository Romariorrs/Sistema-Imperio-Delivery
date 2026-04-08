from django.contrib import admin

from .models import Seller, SellerLeadAssignment


@admin.register(Seller)
class SellerAdmin(admin.ModelAdmin):
    list_display = ("name", "commission_type", "commission_value", "active", "created_at")
    list_filter = ("commission_type", "active")
    search_fields = ("name", "user__username")


@admin.register(SellerLeadAssignment)
class SellerLeadAssignmentAdmin(admin.ModelAdmin):
    list_display = ("seller", "macro_lead", "status", "sequence", "assigned_at", "completed_at")
    list_filter = ("status", "seller")
    search_fields = (
        "seller__name",
        "macro_lead__establishment_name",
        "macro_lead__representative_name",
        "macro_lead__representative_phone",
    )
