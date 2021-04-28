import logging

from src.app.filtering.unstructured_filter import UnstructuredFilter
from src.core.db.models.pdf_models import UnstructuredDocument

logger = logging.getLogger(__name__)


def test_reformat_doc_input():
    unstructured_filter = UnstructuredFilter()

    result_dict = {
        "name": '["Jacky", "B"]',
        "ssn": '["123456789", "987654321"]',
        "dateOfBirth": '["04/07/1999", "03/23/2467"]',
        "zipCode": '["12345", "54321"]',
    }
    doc_input = UnstructuredDocument(**result_dict)
    logger.info(f"before: {doc_input.__dict__}")

    unstructured_filter._reformat_doc_input(doc_input)
    logger.info(f"after: {doc_input.__dict__}")

    assert doc_input.name == ["Jacky", "B"]
    assert doc_input.ssn == ["123456789", "987654321"]
    assert doc_input.dateOfBirth == ["04/07/1999", "03/23/2467"]
    assert doc_input.zipCode == ["12345", "54321"]

