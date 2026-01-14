from django.db import models

from contabilidade.billing.models import Billing
from contabilidade.clients.models import Client


class MessageTemplate(models.Model):
    name = models.CharField(max_length=100)
    body = models.TextField(help_text="Use placeholders como {nome}, {valor}, {vencimento}")

    def __str__(self):
        return self.name


class MessageQueue(models.Model):
    STATUS_CHOICES = (
        ("pending", "Pendente"),
        ("sent", "Enviado"),
        ("error", "Erro"),
    )

    client = models.ForeignKey(Client, on_delete=models.CASCADE)
    billing = models.ForeignKey(Billing, on_delete=models.SET_NULL, null=True, blank=True)
    template = models.ForeignKey(MessageTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    final_text = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    error_message = models.TextField(blank=True)
    attempts = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.client.name} - {self.status}"
