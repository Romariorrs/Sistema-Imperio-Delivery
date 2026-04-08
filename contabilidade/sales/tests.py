from django.contrib.auth.models import User
from django.test import Client, TestCase
from django.urls import reverse

from contabilidade.macros.models import MacroLead
from contabilidade.sales.models import Seller, SellerLeadAssignment


class SellerLeadAssignmentTests(TestCase):
    def setUp(self):
        self.admin_user = User.objects.create_user(
            username="admin-leads",
            password="123456",
            is_staff=True,
        )
        self.seller_user = User.objects.create_user(username="seller-leads", password="123456")
        self.seller = Seller.objects.create(
            user=self.seller_user,
            name="Vendedor Teste",
            commission_type="FIXED",
            commission_value=10,
            active=True,
        )
        self.admin_client = Client()
        self.admin_client.force_login(self.admin_user)
        self.seller_client = Client()
        self.seller_client.force_login(self.seller_user)

    def _create_lead(self, suffix: str, city: str, phone: str):
        return MacroLead.objects.create(
            source="api",
            city=city,
            target_region="Centro",
            establishment_name=f"Loja {suffix}",
            representative_name=f"Contato {suffix}",
            contract_status="Ativo",
            representative_phone=phone,
            representative_phone_norm=f"55{phone}",
            company_category="Restaurante",
            address=f"Rua {suffix}",
            unique_key=f"seller-lead-{suffix}",
        )

    def test_admin_can_assign_filtered_leads_to_seller(self):
        self._create_lead("GO", "Goiania", "62999990000")
        self._create_lead("DF", "Brasilia", "61999990000")
        self._create_lead("SP", "Sao Paulo", "11999990000")

        response = self.admin_client.post(
            reverse("admin_seller_leads"),
            data={
                "seller_id": str(self.seller.id),
                "quantity": "2",
                "ddd_filter": "62,61",
            },
        )

        self.assertEqual(response.status_code, 302)
        assignments = list(self.seller.lead_assignments.select_related("macro_lead").order_by("sequence", "id"))
        self.assertEqual(len(assignments), 2)
        assigned_names = {item.macro_lead.establishment_name for item in assignments}
        self.assertEqual(assigned_names, {"Loja GO", "Loja DF"})

    def test_seller_page_shows_only_current_lead_and_marks_it_viewed(self):
        first = self._create_lead("001", "Goiania", "62999990001")
        second = self._create_lead("002", "Goiania", "62999990002")
        SellerLeadAssignment.objects.create(seller=self.seller, macro_lead=first, sequence=1)
        SellerLeadAssignment.objects.create(seller=self.seller, macro_lead=second, sequence=2)

        response = self.seller_client.get(reverse("seller_leads"))

        self.assertEqual(response.status_code, 200)
        current = response.context["current_assignment"]
        self.assertIsNotNone(current)
        self.assertEqual(current.macro_lead_id, first.id)
        self.assertContains(response, "Loja 001")
        self.assertNotContains(response, "Loja 002")
        first_assignment = SellerLeadAssignment.objects.get(seller=self.seller, macro_lead=first)
        self.assertEqual(first_assignment.status, "viewed")

    def test_seller_next_moves_queue_forward(self):
        first = self._create_lead("101", "Goiania", "62999990101")
        second = self._create_lead("102", "Goiania", "62999990102")
        first_assignment = SellerLeadAssignment.objects.create(
            seller=self.seller,
            macro_lead=first,
            sequence=1,
            status="viewed",
        )
        second_assignment = SellerLeadAssignment.objects.create(
            seller=self.seller,
            macro_lead=second,
            sequence=2,
            status="pending",
        )

        response = self.seller_client.post(
            reverse("seller_leads"),
            data={"assignment_id": str(first_assignment.id), "action": "next"},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        first_assignment.refresh_from_db()
        second_assignment.refresh_from_db()
        self.assertEqual(first_assignment.status, "skipped")
        self.assertEqual(second_assignment.status, "viewed")
        self.assertContains(response, "Loja 102")
        self.assertNotContains(response, "Loja 101")
