import json

from src.tests.utils.factories import (
    PDFIngestionEventFactory,
    PDFUnstructuredDocumentFactory,
    PDFParsingStrategyTypeFactory,
    MainEmployeeFactory,
    MainDocumentTypeFactory,
)


def setup_seed_data():
    PDFParsingStrategyTypeFactory(
        id="9720cb1c-4461-40e4-b800-38dfdfd0061b", name="unstructured"
    )
    PDFIngestionEventFactory(
        id="99b8d772-c0a4-42ac-9bff-fe4409495988",
        s3_bucket="test_bucket",
        s3_key="test_key",
        created_at="2021-03-26 18:54:56.090261",
        updated_at="2021-03-26 18:54:56.090261",
        parsing_strategy_type_id="9720cb1c-4461-40e4-b800-38dfdfd0061b",
    )
    PDFUnstructuredDocumentFactory(
        id="8833fee8-ec15-4829-8d0d-fc7cf4206429",
        name=json.dumps(["Diane", "Meier", "Samantha", "Young"]),
        ssn=json.dumps(["686280445", "195501688"]),
        dateOfBirth=json.dumps(["02/28/2057", "07/13/1981"]),
        text="raw text",
        ingestion_event_id="99b8d772-c0a4-42ac-9bff-fe4409495988",
    )
    MainEmployeeFactory(
        first_name="Diane",
        last_name="Meier",
        ssn="686280445",
        date_of_birth="02/28/2057",
    )
    MainEmployeeFactory(
        first_name="Samantha",
        last_name="Young",
        ssn="195501688",
        date_of_birth="07/13/1980",
    )
    MainDocumentTypeFactory(id="b08ce4d2-9bc8-4975-af86-7e939c1493bb", name="UNKNOWN")

