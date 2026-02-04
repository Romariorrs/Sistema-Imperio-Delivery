from django.urls import path

from .views import macro_api_import, macro_export_csv, macro_export_xlsx, macro_import_csv, macro_list

urlpatterns = [
    path("", macro_list, name="macro_list"),
    path("export/", macro_export_csv, name="macro_export_csv"),
    path("export/xlsx/", macro_export_xlsx, name="macro_export_xlsx"),
    path("import/", macro_import_csv, name="macro_import_csv"),
    path("api/import/", macro_api_import, name="macro_api_import"),
]
