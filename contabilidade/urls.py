from django.contrib import admin
from django.urls import include, path

from contabilidade.views import dashboard, monthly_billing_page, run_monthly_billing_view
from contabilidade.billing.views import asaas_webhook
from contabilidade.admin_views import admin_users

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", dashboard, name="dashboard"),
    path("", include("contabilidade.accounts.urls")),
    path("clients/", include("contabilidade.clients.urls")),
    path("billing/", include("contabilidade.billing.urls")),
    path("billing/mensalidades/", monthly_billing_page, name="monthly_billing_page"),
    path("billing/run-monthly/", run_monthly_billing_view, name="run_monthly_billing"),
    path("messaging/", include("contabilidade.messaging.urls")),
    path("whatsapp/", include("contabilidade.whatsapp.urls")),
    path("api/asaas/webhook/", asaas_webhook, name="asaas_webhook_api"),
    path("seller/", include("contabilidade.sales.urls")),
    path("admin-users/", admin_users, name="admin_users"),
]
