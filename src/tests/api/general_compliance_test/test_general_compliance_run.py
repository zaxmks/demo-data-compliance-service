import re
import logging
import pytest
import datetime

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


class GeneralComplianceTest(DbTestCase):
    def setUp(self):
        from src.tests.api.general_compliance_test.fixture import setup_seed_data

        super().setUp()
        setup_seed_data()

    @requests_mock.Mocker(real_http=True)
    def test_post_dcs_url(self, r_mock):
        matcher = re.compile("mock://somefakeurl/rules_processor/execute/")
        r_mock.register_uri("POST", matcher, text="mocked")
        client = TestClient(pdf_router)

        ingestion_event_id = "ddb8d772-c0a4-42ac-9bff-fe4409495988"

        response = client.post(f"/process/{ingestion_event_id}")

        assert response.status_code == 200

        logger.info(response._content)

        assert response._content == (
            b"Num documents matched: 1, Num employees matched: 1"
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
                .one_or_none()
            )
            employee = (
                context.query(Employee)
                .filter_by(id=compliance_run_event_to_employee.employee_id)
                .one_or_none()
            )
            context.expunge(employee)

        assert employee.first_name == "Jacqueline"
        assert employee.last_name == "Baranov"
        assert employee.ssn == "761870877"
        assert (
            employee.date_of_birth
            == datetime.datetime(year=1971, month=1, day=29).date()
        )
