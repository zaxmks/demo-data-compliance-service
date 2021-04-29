import re
import logging
import pytest

import requests_mock
from fastapi.testclient import TestClient
from src.web.routes.pdf_routes import pdf_router
from src.tests.utils.setup_data import DbTestCase
from src.core.db.db_init import MainDbSession, PdfDbSession
from src.tests.api.general_compliance_test.fixture import (
    setup_main_seed_data,
    setup_pdf_seed_data,
)

logger = logging.getLogger(__name__)


# @pytest.mark.usefixtures("db_session")
class GeneralComplianceTest(DbTestCase):
    def setUp(self):
        super().setUp()
        # TODO: should use self.pdf_db and self.main_db when can
        setup_main_seed_data(MainDbSession)
        setup_pdf_seed_data(PdfDbSession)

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
