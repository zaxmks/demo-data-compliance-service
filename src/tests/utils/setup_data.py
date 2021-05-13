import json
import unittest

from kfai_sql_chemistry.db.main import engines
from kfai_sql_chemistry.utils.setup_for_testing import (
    setup_db_for_tests,
    tear_down_db_for_tests,
)

from src.core.db.models.pdf_models import metadata as pdf_metadata
from src.core.db.models.main_models import metadata as main_metadata
from src.tests.utils.factories import (
    PDFIngestionEventFactory,
    PDFUnstructuredDocumentFactory,
    PDFParsingStrategyTypeFactory,
    MainEmployeeFactory,
    MainDocumentTypeFactory,
)


def teardown_pdf_db():
    tear_down_db_for_tests(engines.get_engine("pdf"))


def setup_pdf_db():
    setup_db_for_tests(engines.get_engine("pdf"), pdf_metadata)
    PDFIngestionEventFactory(
        id="ddb8d772-c0a4-42ac-9bff-fe4409495988",
        s3_bucket="test_bucket",
        s3_key="test_key",
        created_at="2021-03-26 18:54:56.090261",
        updated_at="2021-03-26 18:54:56.090261",
        parsing_strategy_type_id="9720cb1c-4461-40e4-b800-38dfdfd0061b",
    )
    PDFUnstructuredDocumentFactory(
        id="de33fee8-ec15-4829-8d0d-fc7cf4206429",
        name=json.dumps(["Jacqueline", "Baranov"]),
        ssn=json.dumps(["761870877"]),
        dateOfBirth=json.dumps(["01/29/1971"]),
        text="raw text",
        ingestion_event_id="ddb8d772-c0a4-42ac-9bff-fe4409495988",
    )
    PDFParsingStrategyTypeFactory(
        id="9720cb1c-4461-40e4-b800-38dfdfd0061b", name="unstructured"
    )


def teardown_main_db():
    tear_down_db_for_tests(engines.get_engine("main"))


def setup_main_db():
    setup_db_for_tests(engines.get_engine("main"), main_metadata)
    MainEmployeeFactory(
        first_name="Jacqueline",
        last_name="Baranov",
        ssn="761870877",
        date_of_birth="01/29/1971",
    )
    MainDocumentTypeFactory(
        id="b08ce4d2-9bc8-4975-af86-7e939c1493bb", name="UNKNOWN",
    )


class DbTestCase(unittest.TestCase):
    def setUp(self) -> None:
        setup_pdf_db()
        setup_main_db()

    def tearDown(self) -> None:
        teardown_pdf_db()
        teardown_main_db()
