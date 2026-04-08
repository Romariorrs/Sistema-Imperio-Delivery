from django.db.utils import InterfaceError, OperationalError
from django.http import HttpResponse
from django.shortcuts import redirect
from django.urls import reverse


class DatabaseRecoveryMiddleware:
    """
    Evita erro 500 quando o Postgres entra em recuperacao por alguns segundos.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            return self.get_response(request)
        except (OperationalError, InterfaceError):
            return HttpResponse(
                (
                    "<h1>Sistema temporariamente indisponivel</h1>"
                    "<p>O banco de dados esta em recuperacao. "
                    "Aguarde alguns segundos e atualize a pagina.</p>"
                ),
                status=503,
            )


class SellerRedirectMiddleware:
    """
    Redireciona usuarios vendedores (nao staff/superuser) para /seller/.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = request.user
        if user.is_authenticated and hasattr(user, "seller_profile") and not (user.is_staff or user.is_superuser):
            allowed_prefixes = (
                reverse("seller_dashboard"),
                reverse("seller_leads"),
                reverse("seller_client_create"),
                reverse("seller_billing_create"),
                reverse("seller_logout"),
                reverse("seller_login"),
            )
            allowed_starts = tuple(p for p in allowed_prefixes if p)
            if not request.path.startswith(allowed_starts) and not request.path.startswith("/static/"):
                return redirect("seller_dashboard")
        return self.get_response(request)
