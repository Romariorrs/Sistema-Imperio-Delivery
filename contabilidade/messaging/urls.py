from django.urls import path

from .views import (
    queue_clear,
    queue_delete,
    send_mass_messages,
    send_single_message,
    template_create,
    template_list,
    template_update,
)

urlpatterns = [
    path("templates/", template_list, name="template_list"),
    path("templates/new/", template_create, name="template_create"),
    path("templates/<int:pk>/edit/", template_update, name="template_update"),
    path("mass/", send_mass_messages, name="send_mass_messages"),
    path("send/", send_single_message, name="send_single_message"),
    path("queue/<int:pk>/delete/", queue_delete, name="queue_delete"),
    path("queue/clear/", queue_clear, name="queue_clear"),
]
