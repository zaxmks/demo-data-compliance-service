import factory
from factory import fuzzy
import json

from sqlalchemy.orm import scoped_session

from src.core.db.db_init import MainDbSession, PdfDbSession
from src.core.db.models.main_models import DocumentType, Employee
from src.core.db.models.pdf_models import (
    IngestionEvent,
    UnstructuredDocument,
    ParsingStrategyType,
)

main_session = MainDbSession().instance
pdf_session = PdfDbSession().instance

class MainDocumentTypeFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = DocumentType
        sqlalchemy_session = main_session
    id = fuzzy.FuzzyText(length=32)
    name = fuzzy.FuzzyChoice(["UNKNOWN", "FINCEN8300"])

class MainEmployeeFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Employee
        sqlalchemy_session = main_session
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    ssn = factory.Faker("numerify", text="#########")
    date_of_birth = factory.Faker("date", pattern="%m/%d/%Y")

class PDFIngestionEventFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = IngestionEvent
        sqlalchemy_session = pdf_session
    id = fuzzy.FuzzyChoice(["ddb8d772-c0a4-42ac-9bff-fe4409495988"])
    s3_bucket = factory.Faker("text")
    s3_key = factory.Faker("text")
    created_at = factory.Faker("date", pattern="%Y-%m-%d %H:%M:%S.%f")
    updated_at = factory.Faker("date", pattern="%Y-%m-%d %H:%M:%S.%f")
    parsing_strategy_type_id = fuzzy.FuzzyChoice(["9720cb1c-4461-40e4-b800-38dfdfd0061b"])


class PDFUnstructuredDocumentFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = UnstructuredDocument
        sqlalchemy_session = pdf_session
    id = fuzzy.FuzzyChoice(["de33fee8-ec15-4829-8d0d-fc7cf4206429"])
    name = fuzzy.FuzzyChoice([json.dumps(["Jacqueline", "Baranov"])])
    ssn = fuzzy.FuzzyChoice([json.dumps([761870877])])
    text = factory.Faker("text")
    ingestion_event_id = fuzzy.FuzzyChoice(["ddb8d772-c0a4-42ac-9bff-fe4409495988"])

class PDFParsingStrategyTypeFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = ParsingStrategyType
        sqlalchemy_session = pdf_session
    id = fuzzy.FuzzyChoice(["9720cb1c-4461-40e4-b800-38dfdfd0061b"])
    name = fuzzy.FuzzyChoice(["unstructured"])