from django.shortcuts import redirect
from django.urls import reverse


class SellerRedirectMiddleware:
    """
    Redireciona usuários que são vendedores (não staff/superuser) para a área /seller/,
    bloqueando o acesso às demais rotas do sistema.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = request.user
        if user.is_authenticated and hasattr(user, "seller_profile") and not (user.is_staff or user.is_superuser):
            allowed_prefixes = (
                reverse("seller_dashboard"),
                reverse("seller_client_create"),
                reverse("seller_billing_create"),
                reverse("seller_logout"),
                reverse("seller_login"),
            )
            allowed_starts = tuple(p for p in allowed_prefixes if p)
            # permite também acesso a static/admin login
            if not request.path.startswith(allowed_starts) and not request.path.startswith("/static/"):
                return redirect("seller_dashboard")
        response = self.get_response(request)
        return response
