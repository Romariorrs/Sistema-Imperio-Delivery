from django import forms

from datetime import date, timedelta

from contabilidade.clients.models import Client
from contabilidade.billing.models import Billing


class SellerClientForm(forms.ModelForm):
    class Meta:
        model = Client
        fields = ["name", "cpf_cnpj", "email", "phone", "default_amount"]


class SellerBillingForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        seller = kwargs.pop("seller", None)
        super().__init__(*args, **kwargs)
        if seller:
            self.fields["client"].queryset = Client.objects.filter(created_by=seller)
        tomorrow = date.today() + timedelta(days=1)
        self.fields["due_date"].initial = tomorrow
        self.fields["due_date"].widget = forms.DateInput(
            attrs={"type": "date", "min": date.today().isoformat()}
        )
        self.fields["client"].label = "Cliente"
        self.fields["amount"].label = "Valor"
        self.fields["due_date"].label = "Data de vencimento"

    class Meta:
        model = Billing
        fields = ["client", "amount", "due_date"]
