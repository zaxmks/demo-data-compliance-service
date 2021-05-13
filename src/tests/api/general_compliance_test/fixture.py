import json
from src.tests.utils.factories import (
    PDFIngestionEventFactory,
    PDFUnstructuredDocumentFactory,
    PDFParsingStrategyTypeFactory,
    MainEmployeeFactory,
    MainDocumentTypeFactory,
)


def setup_pdf_seed_data():
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
    MainEmployeeFactory(
        first_name="Jacqueline",
        last_name="Baranov",
        ssn="761870877",
        date_of_birth="01/29/1971",
    )
    MainDocumentTypeFactory(
        id="b08ce4d2-9bc8-4975-af86-7e939c1493bb", name="UNKNOWN",
    )
