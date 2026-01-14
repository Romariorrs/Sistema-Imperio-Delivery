from django.contrib import admin

from .models import Client


@admin.register(Client)
class ClientAdmin(admin.ModelAdmin):
    list_display = ("name", "cpf_cnpj", "phone", "default_amount", "active", "asaas_customer_id", "created_by")
    search_fields = ("name", "cpf_cnpj", "email")
    list_filter = ("active", "created_by")
