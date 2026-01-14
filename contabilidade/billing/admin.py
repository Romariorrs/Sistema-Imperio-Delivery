from django.contrib import admin

from .models import Billing


@admin.register(Billing)
class BillingAdmin(admin.ModelAdmin):
    list_display = ("client", "seller", "amount", "due_date", "status", "asaas_billing_id")
    list_filter = ("status", "seller")
    search_fields = ("client__name", "asaas_billing_id", "seller__name")
