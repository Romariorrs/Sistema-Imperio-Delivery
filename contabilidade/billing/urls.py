from django.urls import path

from .views import invoice_single_client, asaas_webhook, reset_finance_view

urlpatterns = [
    path("invoice/<int:client_id>/", invoice_single_client, name="invoice_single_client"),
    path("webhook/", asaas_webhook, name="asaas_webhook"),
    path("reset/", reset_finance_view, name="reset_finance"),
]
