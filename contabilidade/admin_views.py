from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from django.shortcuts import redirect, render

from contabilidade.sales.models import Seller
from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client
from contabilidade.messaging.models import MessageQueue


@login_required
@user_passes_test(lambda u: u.is_superuser)
def admin_users(request):
    users = User.objects.all().order_by("username")
    # Alertas do sistema
    alerts = []
    pending_count = Billing.objects.filter(status="pending").count()
    overdue_count = Billing.objects.filter(status="overdue").count()
    queue_pending = MessageQueue.objects.exclude(status="sent").count()
    missing_asaas = Client.objects.filter(asaas_customer_id__isnull=True).count() + Client.objects.filter(
        asaas_customer_id=""
    ).count()

    if overdue_count > 0:
        alerts.append(
            {
                "title": "Cobranças em atraso",
                "detail": f"{overdue_count} cobrança(s) atrasada(s). Considere reenviar lembrete.",
                "level": "warning",
            }
        )
    if pending_count > 0:
        alerts.append(
            {
                "title": "Cobranças pendentes",
                "detail": f"{pending_count} cobrança(s) aguardando pagamento.",
                "level": "info",
            }
        )
    if queue_pending > 0:
        alerts.append(
            {
                "title": "Fila de mensagens",
                "detail": f"{queue_pending} mensagem(ns) não enviadas na fila WhatsApp.",
                "level": "info",
            }
        )
    if missing_asaas > 0:
        alerts.append(
            {
                "title": "Clientes sem Asaas ID",
                "detail": f"{missing_asaas} cliente(s) sem asaas_customer_id. Cadastre para gerar cobranças.",
                "level": "warning",
            }
        )
    if not alerts:
        alerts.append({"title": "Tudo em dia", "detail": "Nenhum alerta no momento.", "level": "success"})
    edit_id = request.GET.get("edit")
    edit_user = None
    edit_seller = None
    if edit_id:
        try:
            edit_user = User.objects.get(id=edit_id)
            edit_seller = getattr(edit_user, "seller_profile", None)
        except User.DoesNotExist:
            edit_user = None
            edit_seller = None

    if request.method == "POST":
        delete_user_id = request.POST.get("delete_user_id")
        if delete_user_id:
            try:
                user = User.objects.get(id=delete_user_id)
            except User.DoesNotExist:
                messages.error(request, "Usuario nao encontrado.")
                return redirect("admin_users")
            if user.id == request.user.id:
                messages.error(request, "Voce nao pode excluir o proprio usuario.")
                return redirect("admin_users")
            user.delete()
            messages.success(request, "Usuario excluido.")
            return redirect("admin_users")

        user_id = request.POST.get("user_id")
        username = request.POST.get("username")
        password = request.POST.get("password")
        is_staff = bool(request.POST.get("is_staff"))
        is_superuser = bool(request.POST.get("is_superuser"))
        is_seller = bool(request.POST.get("is_seller"))
        seller_name = request.POST.get("seller_name") or username
        commission_type = request.POST.get("commission_type") or "PERCENT"
        commission_value = request.POST.get("commission_value") or "0"

        if not username:
            messages.error(request, "Usuário é obrigatório.")
            return redirect("admin_users")

        # update existing
        if user_id:
            try:
                user = User.objects.get(id=user_id)
            except User.DoesNotExist:
                messages.error(request, "Usuário não encontrado.")
                return redirect("admin_users")
            # se trocar username, verificar duplicidade
            if user.username != username and User.objects.filter(username=username).exists():
                messages.error(request, "Usuário já existe.")
                return redirect("admin_users")
            user.username = username
            if password:
                user.password = make_password(password)
            user.is_staff = is_staff
            user.is_superuser = is_superuser
            user.save()
            # vendedor
            seller_obj = getattr(user, "seller_profile", None)
            if is_seller:
                if seller_obj:
                    seller_obj.name = seller_name
                    seller_obj.commission_type = commission_type
                    seller_obj.commission_value = commission_value
                    seller_obj.active = True
                    seller_obj.save()
                else:
                    Seller.objects.create(
                        user=user,
                        name=seller_name,
                        commission_type=commission_type,
                        commission_value=commission_value,
                        active=True,
                    )
            else:
                if seller_obj:
                    seller_obj.active = False
                    seller_obj.save()
            messages.success(request, "Usuário atualizado.")
            return redirect("admin_users")
        else:
            # create new
            if not password:
                messages.error(request, "Senha é obrigatória para novo usuário.")
                return redirect("admin_users")
            if User.objects.filter(username=username).exists():
                messages.error(request, "Usuário já existe.")
                return redirect("admin_users")
            user = User.objects.create(
                username=username,
                password=make_password(password),
                is_staff=is_staff,
                is_superuser=is_superuser,
            )
            if is_seller:
                Seller.objects.create(
                    user=user,
                    name=seller_name,
                    commission_type=commission_type,
                    commission_value=commission_value,
                    active=True,
                )
            messages.success(request, "Usuário criado.")
            return redirect("admin_users")

    return render(
        request,
        "admin_users.html",
        {
            "users": users,
            "commission_choices": Seller.COMMISSION_CHOICES,
            "edit_user": edit_user,
            "edit_seller": edit_seller,
            "alerts": alerts,
        },
    )
