from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.db import close_old_connections
from django.db.utils import InterfaceError, OperationalError
from django.shortcuts import redirect, render


def login_view(request):
    if request.user.is_authenticated:
        return redirect("dashboard")

    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        try:
            close_old_connections()
            user = authenticate(request, username=username, password=password)
            if user is not None:
                login(request, user)
                return redirect("dashboard")
        except (OperationalError, InterfaceError):
            messages.error(
                request,
                "Banco temporariamente indisponivel. Aguarde alguns segundos e tente novamente.",
            )
            return render(request, "accounts/login.html")

        messages.error(request, "Usuario ou senha invalidos.")

    return render(request, "accounts/login.html")


def logout_view(request):
    logout(request)
    return redirect("login")
