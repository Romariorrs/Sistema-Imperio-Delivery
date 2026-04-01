from django.db import models

from contabilidade.clients.models import Client
from contabilidade.sales.models import Seller


class Billing(models.Model):
    STATUS_CHOICES = (
        ("pending", "Pendente"),
        ("paid", "Pago"),
        ("overdue", "Atrasado"),
        ("canceled", "Cancelado"),
    )
    BILLING_TYPE_CHOICES = (
        ("CREDIT_CARD", "Cartao de credito"),
        ("PIX", "Pix"),
        ("BOLETO", "Boleto"),
    )
    CHARGE_TYPE_CHOICES = (
        ("DETACHED", "Avulsa"),
        ("INSTALLMENT", "Parcelada"),
        ("RECURRENT", "Recorrente"),
    )

    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="billings")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    due_date = models.DateField()
    subscription_end_date = models.DateField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    billing_type = models.CharField(
        max_length=30, choices=BILLING_TYPE_CHOICES, default="CREDIT_CARD"
    )
    charge_type = models.CharField(max_length=30, choices=CHARGE_TYPE_CHOICES, default="RECURRENT")
    recurring_months = models.PositiveIntegerField(default=1)
    seller = models.ForeignKey(Seller, on_delete=models.SET_NULL, null=True, blank=True)
    asaas_billing_id = models.CharField(max_length=100, blank=True)
    asaas_checkout_id = models.CharField(max_length=100, blank=True)
    asaas_subscription_id = models.CharField(max_length=100, blank=True)
    payment_link = models.URLField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.client.name} - {self.amount} - {self.get_charge_type_display()} - {self.status}"
