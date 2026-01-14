from django.contrib import admin

from .models import Seller


@admin.register(Seller)
class SellerAdmin(admin.ModelAdmin):
    list_display = ("name", "commission_type", "commission_value", "active", "created_at")
    list_filter = ("commission_type", "active")
    search_fields = ("name", "user__username")
