from django.db import models
from django.contrib.auth import get_user_model


class MacroLead(models.Model):
    source = models.CharField(max_length=50, default="gattaran", db_index=True)
    city = models.CharField(max_length=255, blank=True, db_index=True)
    target_region = models.CharField(max_length=255, blank=True, db_index=True)
    lead_created_at = models.DateTimeField(null=True, blank=True, db_index=True)
    establishment_name = models.CharField(max_length=255, blank=True, db_index=True)
    representative_name = models.CharField(max_length=255, blank=True, db_index=True)
    contract_status = models.CharField(max_length=100, blank=True, db_index=True)
    business_99_status = models.CharField(max_length=100, blank=True, db_index=True)
    representative_phone = models.CharField(max_length=50, blank=True, db_index=True)
    representative_phone_norm = models.CharField(max_length=20, blank=True, db_index=True)
    is_blocked_number = models.BooleanField(default=False, db_index=True)
    company_category = models.CharField(max_length=255, blank=True, db_index=True)
    address = models.TextField(blank=True)
    unique_key = models.CharField(max_length=64, unique=True, db_index=True)
    first_seen_at = models.DateTimeField(auto_now_add=True)
    last_seen_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-last_seen_at"]

    def __str__(self):
        name = self.establishment_name or "Lead"
        city = f" - {self.city}" if self.city else ""
        return f"{name}{city}"


class MacroRun(models.Model):
    RUN_TYPE_CHOICES = (
        ("command", "Comando"),
        ("api", "API"),
        ("csv", "CSV"),
    )
    STATUS_CHOICES = (
        ("running", "Executando"),
        ("success", "Sucesso"),
        ("error", "Erro"),
    )

    run_type = models.CharField(max_length=20, choices=RUN_TYPE_CHOICES, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="running", db_index=True)
    source = models.CharField(max_length=50, default="gattaran")
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    triggered_by = models.ForeignKey(
        get_user_model(),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="macro_runs",
    )
    request_ip = models.GenericIPAddressField(null=True, blank=True)
    total_collected = models.PositiveIntegerField(default=0)
    total_received = models.PositiveIntegerField(default=0)
    total_sent = models.PositiveIntegerField(default=0)
    created_count = models.PositiveIntegerField(default=0)
    updated_count = models.PositiveIntegerField(default=0)
    ignored_count = models.PositiveIntegerField(default=0)
    invalid_count = models.PositiveIntegerField(default=0)
    message = models.TextField(blank=True)

    class Meta:
        ordering = ["-started_at"]

    def __str__(self):
        return f"{self.get_run_type_display()} - {self.get_status_display()} ({self.started_at:%d/%m %H:%M})"
