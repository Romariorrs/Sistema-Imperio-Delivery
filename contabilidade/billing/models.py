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

    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="billings")
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    due_date = models.DateField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    seller = models.ForeignKey(Seller, on_delete=models.SET_NULL, null=True, blank=True)
    asaas_billing_id = models.CharField(max_length=100, blank=True)
    payment_link = models.URLField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.client.name} - {self.amount} - {self.status}"
