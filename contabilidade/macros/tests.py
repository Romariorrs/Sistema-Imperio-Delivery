import json
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from contabilidade.macros.models import MacroLead, MacroRun
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

    def test_upsert_parses_lead_created_at(self):
        row = {
            "Cidade": "Sao Paulo",
            "Nome do estabelecimento": "Loja Data",
            "Telefone do representante do estabelecimento": "11999990000",
            "Horario de criacao do lead": "2026-02-02 13:45:20 UTC-3",
            "Seu Negocio na 99": "Nao ativado",
        }
        upsert_rows([row], default_source="api")
        lead = MacroLead.objects.first()
        self.assertIsNotNone(lead.lead_created_at)
        self.assertEqual(lead.business_99_status, "Nao ativado")

    def test_upsert_trims_oversized_values(self):
        row = {
            "Cidade": "S" * 400,
            "Nome do estabelecimento": "L" * 400,
            "Status do contrato": "A" * 150,
            "Telefone do representante do estabelecimento": "9" * 80,
            "Categoria da empresa": "C" * 400,
            "Endereco": "Rua X",
        }
        result = upsert_rows([row], default_source="api")
        self.assertEqual(result["created"], 1)
        lead = MacroLead.objects.first()
        self.assertEqual(len(lead.city), 255)
        self.assertEqual(len(lead.establishment_name), 255)
        self.assertEqual(len(lead.contract_status), 100)
        self.assertEqual(len(lead.representative_phone), 50)
        self.assertEqual(len(lead.company_category), 255)


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

    def test_api_import_marks_error_if_processing_fails(self):
        payload = [{"Cidade": "Rio", "Nome do estabelecimento": "Loja API"}]
        with patch("contabilidade.macros.views.upsert_rows", side_effect=RuntimeError("boom")):
            resp = self.client.post(
                self.url,
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_AUTHORIZATION="Bearer token123",
            )
        self.assertEqual(resp.status_code, 500)
        run = MacroRun.objects.first()
        self.assertIsNotNone(run)
        self.assertEqual(run.status, "error")

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
        collect_resp = self.client.get(reverse("macro_collect"))
        self.assertEqual(collect_resp.status_code, 200)

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

    def test_delete_filtered_leads(self):
        MacroLead.objects.create(
            source="api",
            city="Rio de Janeiro",
            target_region="R1",
            establishment_name="Loja RJ",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999998888",
            representative_phone_norm="5521999998888",
            company_category="Brasileira",
            address="Rua A",
            unique_key="del-1",
        )
        MacroLead.objects.create(
            source="api",
            city="Sao Paulo",
            target_region="R2",
            establishment_name="Loja SP",
            representative_name="Joao",
            contract_status="Ativo",
            representative_phone="11999998888",
            representative_phone_norm="5511999998888",
            company_category="Pizza",
            address="Rua B",
            unique_key="del-2",
        )
        resp = self.client.post(
            reverse("macro_delete_filtered"),
            data={"city": "Rio de Janeiro", "confirm_text": "EXCLUIR"},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(MacroLead.objects.count(), 1)
        self.assertEqual(MacroLead.objects.first().city, "Sao Paulo")

    def test_delete_all_and_runs(self):
        MacroLead.objects.create(
            source="api",
            city="Campinas",
            target_region="R3",
            establishment_name="Loja C",
            representative_name="Maria",
            contract_status="Pendente",
            representative_phone="19999998888",
            representative_phone_norm="5519999998888",
            company_category="Lanches",
            address="Rua C",
            unique_key="del-3",
        )
        MacroRun.objects.create(
            run_type="csv",
            status="success",
            source="csv",
            message="ok",
        )
        run_resp = self.client.post(
            reverse("macro_delete_runs"),
            data={"confirm_text": "LIMPAR HISTORICO", "next": "collect"},
        )
        all_resp = self.client.post(
            reverse("macro_delete_all"),
            data={"confirm_text": "APAGAR TUDO", "next": "collect"},
        )
        self.assertEqual(run_resp.status_code, 302)
        self.assertEqual(all_resp.status_code, 302)
        self.assertEqual(MacroRun.objects.count(), 0)
        self.assertEqual(MacroLead.objects.count(), 0)

    def test_delete_specific_source(self):
        MacroLead.objects.create(
            source="csv",
            city="Rio",
            target_region="R1",
            establishment_name="Loja CSV",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999990000",
            representative_phone_norm="5521999990000",
            company_category="Brasileira",
            address="Rua 1",
            unique_key="src-1",
        )
        MacroLead.objects.create(
            source="api",
            city="Rio",
            target_region="R2",
            establishment_name="Loja API",
            representative_name="Joao",
            contract_status="Ativo",
            representative_phone="21999991111",
            representative_phone_norm="5521999991111",
            company_category="Pizza",
            address="Rua 2",
            unique_key="src-2",
        )
        resp = self.client.post(
            reverse("macro_delete_source"),
            data={"source": "csv", "confirm_text": "EXCLUIR BASE", "next": "database"},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(MacroLead.objects.count(), 1)
        self.assertEqual(MacroLead.objects.first().source, "api")

    def test_delete_specific_run_item(self):
        run = MacroRun.objects.create(
            run_type="csv",
            status="success",
            source="csv",
            message="ok",
        )
        resp = self.client.post(
            reverse("macro_delete_run_item", args=[run.id]),
            data={"next": "collect"},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertFalse(MacroRun.objects.filter(id=run.id).exists())

    def test_block_and_unblock_phone_updates_same_number(self):
        lead_a = MacroLead.objects.create(
            source="api",
            city="Rio",
            target_region="R1",
            establishment_name="Loja A",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999990000",
            representative_phone_norm="5521999990000",
            company_category="Brasileira",
            address="Rua 1",
            unique_key="block-1",
        )
        lead_b = MacroLead.objects.create(
            source="api",
            city="Niteroi",
            target_region="R2",
            establishment_name="Loja B",
            representative_name="Bia",
            contract_status="Ativo",
            representative_phone="(21) 99999-0000",
            representative_phone_norm="5521999990000",
            company_category="Pizza",
            address="Rua 2",
            unique_key="block-2",
        )

        block_resp = self.client.post(reverse("macro_block_phone", args=[lead_a.id]), data={})
        self.assertEqual(block_resp.status_code, 302)
        self.assertEqual(MacroLead.objects.filter(is_blocked_number=True).count(), 2)

        unblock_resp = self.client.post(reverse("macro_unblock_phone", args=[lead_b.id]), data={})
        self.assertEqual(unblock_resp.status_code, 302)
        self.assertEqual(MacroLead.objects.filter(is_blocked_number=True).count(), 0)

    def test_delete_blocked_respects_filter(self):
        MacroLead.objects.create(
            source="api",
            city="Rio",
            target_region="R1",
            establishment_name="Loja Bloq",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999990000",
            representative_phone_norm="5521999990000",
            is_blocked_number=True,
            company_category="Brasileira",
            address="Rua 1",
            unique_key="blocked-del-1",
        )
        MacroLead.objects.create(
            source="api",
            city="Sao Paulo",
            target_region="R2",
            establishment_name="Loja Livre",
            representative_name="Joao",
            contract_status="Ativo",
            representative_phone="11999998888",
            representative_phone_norm="5511999998888",
            is_blocked_number=False,
            company_category="Pizza",
            address="Rua 2",
            unique_key="blocked-del-2",
        )
        resp = self.client.post(
            reverse("macro_delete_blocked"),
            data={"blocked": "yes", "confirm_text": "EXCLUIR BLOQUEADOS"},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(MacroLead.objects.count(), 1)
        self.assertEqual(MacroLead.objects.first().city, "Sao Paulo")

    def test_filter_duplicate_phones(self):
        MacroLead.objects.create(
            source="api",
            city="Rio",
            target_region="R1",
            establishment_name="Loja Dup 1",
            representative_name="Ana",
            contract_status="Ativo",
            representative_phone="21999990000",
            representative_phone_norm="5521999990000",
            company_category="Brasileira",
            address="Rua 1",
            unique_key="dup-filter-1",
        )
        MacroLead.objects.create(
            source="api",
            city="Niteroi",
            target_region="R2",
            establishment_name="Loja Dup 2",
            representative_name="Bia",
            contract_status="Ativo",
            representative_phone="(21) 99999-0000",
            representative_phone_norm="5521999990000",
            company_category="Pizza",
            address="Rua 2",
            unique_key="dup-filter-2",
        )
        MacroLead.objects.create(
            source="api",
            city="Sao Paulo",
            target_region="R3",
            establishment_name="Loja Unica",
            representative_name="Joao",
            contract_status="Ativo",
            representative_phone="11999998888",
            representative_phone_norm="5511999998888",
            company_category="Lanches",
            address="Rua 3",
            unique_key="dup-filter-3",
        )

        duplicates_resp = self.client.get(reverse("macro_list"), data={"phone_dup": "duplicates"})
        duplicates_page = list(duplicates_resp.context["page_obj"].object_list)
        self.assertEqual(duplicates_resp.status_code, 200)
        self.assertEqual(len(duplicates_page), 2)

        unique_resp = self.client.get(reverse("macro_list"), data={"phone_dup": "unique"})
        unique_page = list(unique_resp.context["page_obj"].object_list)
        self.assertEqual(unique_resp.status_code, 200)
        self.assertEqual(len(unique_page), 1)
        self.assertEqual(unique_page[0].city, "Sao Paulo")
