import re
import logging
import pytest

import requests_mock
from fastapi.testclient import TestClient
from src.web.routes.pdf_routes import pdf_router
from src.tests.utils.setup_data import DbTestCase
from src.core.db.db_init import MainDbSession, PdfDbSession
from src.core.db.models.main_models import (
    ComplianceRunEvent,
    Employee,
    EmployeeToComplianceRunEvent,
)

logger = logging.getLogger(__name__)


class MultipleAffiliatesTest(DbTestCase):
    def setUp(self):
        from src.tests.api.multiple_affiliates_test.multiple_affiliates_fixture import (
            setup_seed_data,
        )

        super().setUp()
        setup_seed_data()

    @requests_mock.Mocker(real_http=True)
    def test_post_dcs_url(self, r_mock):
        matcher = re.compile("mock://somefakeurl/rules_processor/execute/")
        r_mock.register_uri("POST", matcher, text="mocked")
        client = TestClient(pdf_router)

        ingestion_event_id = "99b8d772-c0a4-42ac-9bff-fe4409495988"

        response = client.post(f"/process/{ingestion_event_id}")

        assert response.status_code == 200

        logger.info(response._content)

        assert response._content == (
            b"Num documents matched: 1, Num employees matched: 2"
        )

        with MainDbSession() as context:
            compliance_run_event = (
                context.query(ComplianceRunEvent)
                .filter_by(id=ingestion_event_id)
                .one_or_none()
            )
            compliance_run_event_to_employee = (
                context.query(EmployeeToComplianceRunEvent)
                .filter_by(compliance_run_event_id=compliance_run_event.id)
                .all()
            )
            assert len(compliance_run_event_to_employee) == 2
            employees = (
                context.query(Employee)
                .join(EmployeeToComplianceRunEvent)
                .filter(Employee.id == EmployeeToComplianceRunEvent.employee_id)
                .all()
            )
            logger.info(employees)
            assert len(employees) == 2

            # employees could be in any order
            e0 = employees[0]
            e1 = employees[1]
            a = (
                e0.first_name == "Diane"
                and e0.last_name == "Meier"
                and e1.first_name == "Samantha"
                and e1.last_name == "Young"
            )
            b = (
                e1.first_name == "Diane"
                and e1.last_name == "Meier"
                and e0.first_name == "Samantha"
                and e0.last_name == "Young"
            )
            assert a or b
