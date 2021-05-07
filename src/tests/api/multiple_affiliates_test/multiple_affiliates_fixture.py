import json

from src.core.db.models.main_models import DocumentType, Employee
from src.core.db.models.pdf_models import (
    IngestionEvent,
    UnstructuredDocument,
    ParsingStrategyType,
)


def setup_pdf_seed_data(pdf_db_session):
    with pdf_db_session() as context:
        ingestion_event = IngestionEvent(
            id="99b8d772-c0a4-42ac-9bff-fe4409495988",
            s3_bucket="test_bucket",
            s3_key="test_key",
            created_at="2021-03-26 18:54:56.090261",
            updated_at="2021-03-26 18:54:56.090261",
            parsing_strategy_type_id="9720cb1c-4461-40e4-b800-38dfdfd0061b",
        )
        unstructured = UnstructuredDocument(
            id="8833fee8-ec15-4829-8d0d-fc7cf4206429",
            name=json.dumps(["Diane", "Meier", "Samantha", "Young"]),
            ssn=json.dumps(["686280445", "195501688"]),
            dateOfBirth=json.dumps(["2/28/2057", "7/13/1981"]),
            text="raw text",
            ingestion_event_id="99b8d772-c0a4-42ac-9bff-fe4409495988",
        )
        parsing_strategy_type = ParsingStrategyType(
            id="9720cb1c-4461-40e4-b800-38dfdfd0061b", name="unstructured"
        )

        context.add(parsing_strategy_type)
        context.commit()

        context.add(ingestion_event)
        context.commit()

        context.add(unstructured)
        context.commit()


def setup_main_seed_data(main_db_session):
    with main_db_session() as context:
        employee1 = Employee(
            first_name="Diane",
            last_name="Meier",
            ssn="686280445",
            date_of_birth="2/28/2057",
        )
        employee2 = Employee(
            first_name="Samantha",
            last_name="Young",
            ssn="195501688",
            date_of_birth="7/13/1980",
        )
        document_type = DocumentType(
            id="b08ce4d2-9bc8-4975-af86-7e939c1493bb", name="UNKNOWN"
        )
        context.add_all([employee1, employee2])
        context.add(document_type)
        context.commit()
