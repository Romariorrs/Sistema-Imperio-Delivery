from django.urls import path

from .views import (
    whatsapp_reset,
    whatsapp_send_now,
    whatsapp_status,
    whatsapp_status_json,
    whatsapp_worker_status,
    whatsapp_start_visible,
)

urlpatterns = [
    path("", whatsapp_status, name="whatsapp_status"),
    path("status/", whatsapp_status_json, name="whatsapp_status_json"),
    path("send-now/", whatsapp_send_now, name="whatsapp_send_now"),
    path("reset/", whatsapp_reset, name="whatsapp_reset"),
    path("worker-status/", whatsapp_worker_status, name="whatsapp_worker_status"),
    path("start-visible/", whatsapp_start_visible, name="whatsapp_start_visible"),
]
