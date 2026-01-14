from django.urls import path

from .views import (
    client_create,
    client_delete,
    client_export,
    client_import,
    client_list,
    client_update,
)

urlpatterns = [
    path("", client_list, name="client_list"),
    path("new/", client_create, name="client_create"),
    path("<int:pk>/edit/", client_update, name="client_update"),
    path("<int:pk>/delete/", client_delete, name="client_delete"),
    path("import/", client_import, name="client_import"),
    path("export/", client_export, name="client_export"),
]
