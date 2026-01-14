from django.conf import settings
from django.db import models


class Client(models.Model):
    name = models.CharField(max_length=255)
    cpf_cnpj = models.CharField(max_length=20, unique=True)
    phone = models.CharField(max_length=20, blank=True)
    email = models.EmailField(blank=True)
    default_amount = models.DecimalField(
        max_digits=10, decimal_places=2, default=0, verbose_name="Valor da mensalidade"
    )
    active = models.BooleanField(default=True)
    asaas_customer_id = models.CharField(max_length=50, blank=True)
    created_by = models.ForeignKey(
        "sales.Seller", on_delete=models.SET_NULL, null=True, blank=True, related_name="clients"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name
