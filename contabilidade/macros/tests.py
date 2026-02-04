import json

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from contabilidade.macros.models import MacroLead
from contabilidade.macros.services import upsert_rows


class MacroServicesTests(TestCase):
    def test_upsert_deduplicates_with_stable_key(self):
        first = {
            "Cidade": "Sao Paulo",
            "Nome do estabelecimento": "Loja Teste",
            "Telefone do representante do estabelecimento": "(11) 99999-0000",
            "Status do contrato": "Ativo",
        }
        second = {
            "Cidade": "Sao Paulo",
            "Nome do estabelecimento": "Loja Teste",
            "Telefone do representante do estabelecimento": "11999990000",
            "Status do contrato": "Pendente",
        }
        upsert_rows([first], default_source="api")
        result = upsert_rows([second], default_source="api")
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["updated"], 1)
        self.assertEqual(MacroLead.objects.count(), 1)
        self.assertEqual(MacroLead.objects.first().contract_status, "Pendente")


@override_settings(
    MACRO_API_TOKEN="token123",
    MACRO_API_ALLOWED_IPS=[],
    MACRO_API_RATE_LIMIT_PER_MINUTE=100,
)
class MacroApiImportTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = Client()
        self.url = reverse("macro_api_import")

    def test_api_import_with_token(self):
        payload = [{"Cidade": "Rio", "Nome do estabelecimento": "Loja API"}]
        resp = self.client.post(
            self.url,
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token123",
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(MacroLead.objects.filter(establishment_name="Loja API").count(), 1)

    @override_settings(MACRO_API_RATE_LIMIT_PER_MINUTE=1)
    def test_api_rate_limit_blocks_second_request(self):
        payload = [{"Cidade": "Rio", "Nome do estabelecimento": "Loja 1"}]
        first = self.client.post(
            self.url,
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token123",
        )
        second = self.client.post(
            self.url,
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token123",
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)


class MacroScreenTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="staff", password="123456", is_staff=True)
        self.client = Client()
        self.client.login(username="staff", password="123456")

    def test_macro_page_requires_staff(self):
        resp = self.client.get(reverse("macro_list"))
        self.assertEqual(resp.status_code, 200)

    def test_export_xlsx_works(self):
        MacroLead.objects.create(
            source="api",
            city="Rio",
            target_region="Centro",
            establishment_name="Loja X",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999998888",
            representative_phone_norm="5521999998888",
            company_category="Restaurante",
            address="Rua Teste",
            unique_key="k1",
        )
        resp = self.client.get(reverse("macro_export_xlsx"))
        self.assertEqual(resp.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resp["Content-Type"],
        )

    def test_download_local_agent_files(self):
        exe_resp = self.client.get(reverse("macro_download_local_agent_exe"))
        mac_resp = self.client.get(reverse("macro_download_local_agent_mac"))
        py_resp = self.client.get(reverse("macro_download_local_agent_py"))
        bat_resp = self.client.get(reverse("macro_download_local_agent_bat"))
        self.assertIn(exe_resp.status_code, (200, 404))
        self.assertEqual(mac_resp.status_code, 200)
        self.assertIn("attachment; filename=", mac_resp["Content-Disposition"])
        self.assertEqual(py_resp.status_code, 200)
        self.assertEqual(bat_resp.status_code, 200)
        self.assertIn("attachment; filename=", py_resp["Content-Disposition"])
        self.assertIn("attachment; filename=", bat_resp["Content-Disposition"])
