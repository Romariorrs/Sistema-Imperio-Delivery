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


class SellerLeadAssignment(models.Model):
    ACTIVE_STATUSES = ("pending", "viewed")
    STATUS_CHOICES = (
        ("pending", "Pendente"),
        ("viewed", "Em andamento"),
        ("completed", "Concluido"),
        ("skipped", "Pulou"),
    )

    seller = models.ForeignKey(Seller, on_delete=models.CASCADE, related_name="lead_assignments")
    macro_lead = models.ForeignKey(
        "macros.MacroLead", on_delete=models.CASCADE, related_name="seller_assignments"
    )
    assigned_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="lead_assignments_created",
    )
    sequence = models.PositiveIntegerField(default=0, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending", db_index=True)
    assigned_at = models.DateTimeField(auto_now_add=True)
    first_viewed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["sequence", "id"]
        constraints = [
            models.UniqueConstraint(fields=["seller", "macro_lead"], name="unique_seller_macro_lead")
        ]

    def __str__(self):
        lead_name = self.macro_lead.establishment_name or self.macro_lead.representative_name or "Lead"
        return f"{self.seller.name} - {lead_name} - {self.get_status_display()}"
