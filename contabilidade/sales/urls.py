from django.urls import path

from contabilidade.sales.views import (
    seller_billing_create,
    seller_client_create,
    seller_dashboard,
    seller_login,
    seller_logout,
)

urlpatterns = [
    path("login/", seller_login, name="seller_login"),
    path("logout/", seller_logout, name="seller_logout"),
    path("dashboard/", seller_dashboard, name="seller_dashboard"),
    path("clients/new/", seller_client_create, name="seller_client_create"),
    path("billing/new/", seller_billing_create, name="seller_billing_create"),
]
