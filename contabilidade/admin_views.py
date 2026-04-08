from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib.auth.hashers import make_password
from django.db import transaction
from django.db.models import Count, Max, Q
from django.shortcuts import redirect, render
from django.urls import reverse

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client
from contabilidade.macros.views import _apply_filters, _base_macrolead_queryset, _macrolead_has_columns
from contabilidade.messaging.models import MessageQueue
from contabilidade.sales.models import Seller, SellerLeadAssignment


LEAD_FILTER_FIELDS = (
    "q",
    "ddd_filter",
    "city",
    "contract_status",
    "business_99_status",
    "company_category",
    "blocked",
)


def _staff_access(user):
    return user.is_staff or user.is_superuser


def _lead_filter_params(source):
    return {field: (source.get(field) or "").strip() for field in LEAD_FILTER_FIELDS}


def _available_leads_queryset(params):
    queryset = _apply_filters(queryset=_base_macrolead_queryset(), params=params)
    if _macrolead_has_columns("representative_phone_norm"):
        queryset = queryset.exclude(Q(representative_phone="") & Q(representative_phone_norm=""))
    else:
        queryset = queryset.exclude(representative_phone="")
    if _macrolead_has_columns("is_blocked_number") and not params.get("blocked"):
        queryset = queryset.filter(is_blocked_number=False)
    assigned_ids = SellerLeadAssignment.objects.values_list("macro_lead_id", flat=True)
    return queryset.exclude(id__in=assigned_ids)


def _seller_lead_summaries(sellers):
    rows = SellerLeadAssignment.objects.filter(seller__in=sellers).values("seller_id", "status").annotate(
        total=Count("id")
    )
    summary_map = {
        seller.id: {"pending": 0, "viewed": 0, "completed": 0, "skipped": 0, "total": 0}
        for seller in sellers
    }
    for row in rows:
        status = row["status"]
        total = row["total"]
        summary = summary_map[row["seller_id"]]
        summary[status] = total
        summary["total"] += total

    summaries = []
    for seller in sellers:
        summary = summary_map[seller.id]
        summaries.append(
            {
                "seller": seller,
                "pending": summary["pending"],
                "viewed": summary["viewed"],
                "completed": summary["completed"],
                "skipped": summary["skipped"],
                "remaining": summary["pending"] + summary["viewed"],
                "total": summary["total"],
            }
        )
    return summaries


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


@login_required
@user_passes_test(_staff_access)
def admin_seller_leads(request):
    sellers = list(Seller.objects.filter(active=True).select_related("user").order_by("name"))
    filter_source = request.POST if request.method == "POST" else request.GET
    filter_params = _lead_filter_params(filter_source)

    if request.method == "POST":
        seller_id = (request.POST.get("seller_id") or "").strip()
        quantity_raw = (request.POST.get("quantity") or "").strip()
        replace_pending = bool(request.POST.get("replace_pending"))
        seller = next((item for item in sellers if str(item.id) == seller_id), None)

        try:
            quantity = int(quantity_raw)
        except (TypeError, ValueError):
            quantity = 0

        if seller is None:
            messages.error(request, "Selecione um vendedor ativo.")
        elif quantity <= 0:
            messages.error(request, "Informe uma quantidade valida de leads.")
        else:
            with transaction.atomic():
                if replace_pending:
                    seller.lead_assignments.filter(
                        status__in=SellerLeadAssignment.ACTIVE_STATUSES
                    ).delete()
                available_qs = _available_leads_queryset(filter_params)
                selected_leads = list(available_qs[:quantity])
                if not selected_leads:
                    messages.error(request, "Nenhum lead disponivel para os filtros informados.")
                else:
                    last_sequence = seller.lead_assignments.aggregate(max_sequence=Max("sequence"))[
                        "max_sequence"
                    ] or 0
                    SellerLeadAssignment.objects.bulk_create(
                        [
                            SellerLeadAssignment(
                                seller=seller,
                                macro_lead=lead,
                                assigned_by=request.user,
                                sequence=last_sequence + index,
                            )
                            for index, lead in enumerate(selected_leads, start=1)
                        ]
                    )
                    messages.success(
                        request,
                        f"{len(selected_leads)} lead(s) enviados para {seller.name}.",
                    )

            querystring = urlencode({key: value for key, value in filter_params.items() if value})
            target = reverse("admin_seller_leads")
            if querystring:
                target = f"{target}?{querystring}"
            return redirect(target)

    base_queryset = _base_macrolead_queryset()
    city_options = (
        base_queryset.exclude(city="").values_list("city", flat=True).distinct().order_by("city")
    )
    contract_status_options = (
        base_queryset.exclude(contract_status="")
        .values_list("contract_status", flat=True)
        .distinct()
        .order_by("contract_status")
    )
    category_options = (
        base_queryset.exclude(company_category="")
        .values_list("company_category", flat=True)
        .distinct()
        .order_by("company_category")
    )
    business_99_options = ()
    if _macrolead_has_columns("business_99_status"):
        business_99_options = (
            base_queryset.exclude(business_99_status="")
            .values_list("business_99_status", flat=True)
            .distinct()
            .order_by("business_99_status")
        )

    available_qs = _available_leads_queryset(filter_params)
    context = {
        "sellers": sellers,
        "seller_summaries": _seller_lead_summaries(sellers),
        "available_count": available_qs.count(),
        "preview_leads": available_qs[:12],
        "filter_params": filter_params,
        "selected_seller_id": (request.POST.get("seller_id") or request.GET.get("seller_id") or "").strip(),
        "selected_quantity": (request.POST.get("quantity") or request.GET.get("quantity") or "20").strip(),
        "replace_pending": bool(request.POST.get("replace_pending")),
        "city_options": city_options,
        "contract_status_options": contract_status_options,
        "category_options": category_options,
        "business_99_options": business_99_options,
        "blocked_enabled": _macrolead_has_columns("is_blocked_number"),
        "business_99_enabled": _macrolead_has_columns("business_99_status"),
    }
    return render(request, "admin_seller_leads.html", context)
