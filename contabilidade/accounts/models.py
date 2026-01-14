from django.contrib.auth.models import User
from django.db import models


class Employee(models.Model):
    ROLE_CHOICES = (
        ("admin", "Admin"),
        ("operacional", "Operacional"),
    )

    user = models.OneToOneField(User, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.username} ({self.get_role_display()})"
