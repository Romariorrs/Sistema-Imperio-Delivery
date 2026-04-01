from datetime import date, timedelta

from django import forms

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client


class SellerClientForm(forms.ModelForm):
    class Meta:
        model = Client
        fields = [
            "name",
            "cpf_cnpj",
            "email",
            "phone",
            "postal_code",
            "address",
            "address_number",
            "complement",
            "province",
            "default_amount",
            "recurring_months",
        ]


class SellerBillingForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        seller = kwargs.pop("seller", None)
        initial_client = kwargs.pop("initial_client", None)
        super().__init__(*args, **kwargs)
        queryset = Client.objects.filter(active=True).order_by("name")
        if seller:
            queryset = queryset.filter(created_by=seller)
        self.fields["client"].queryset = queryset

        tomorrow = date.today() + timedelta(days=1)
        self.fields["due_date"].initial = tomorrow
        self.fields["due_date"].widget = forms.DateInput(
            attrs={"type": "date", "min": date.today().isoformat()}
        )
        self.fields["recurring_months"].widget = forms.NumberInput(attrs={"min": 1, "step": 1})
        self.fields["client"].label = "Cliente cadastrado"
        self.fields["amount"].label = "Valor mensal"
        self.fields["due_date"].label = "Primeiro vencimento"
        self.fields["recurring_months"].label = "Meses de recorrencia"

        if initial_client and not self.is_bound:
            client = queryset.filter(pk=initial_client).first()
            if client:
                self.fields["client"].initial = client
                self.fields["amount"].initial = client.default_amount
                self.fields["recurring_months"].initial = client.recurring_months

    class Meta:
        model = Billing
        fields = ["client", "amount", "due_date", "recurring_months"]
