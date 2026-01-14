from django.contrib.auth.models import User
from django.db import models


class Seller(models.Model):
    COMMISSION_CHOICES = (
        ("FIXED", "Valor fixo"),
        ("PERCENT", "Percentual"),
    )

    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="seller_profile")
    name = models.CharField(max_length=255)
    commission_type = models.CharField(max_length=10, choices=COMMISSION_CHOICES, default="PERCENT")
    commission_value = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
