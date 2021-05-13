import logging

from src.app.document_type_enum import DocumentTypeEnum
from src.app.output.output_db_writer_fincen import OutputDBWriterFincen
from src.app.output.output_db_writer_unknown import OutputDBWriterUnknown

from src.log_util import log

logger = logging.getLogger(__name__)

class OutputDBWriterFactory(object):

    def build_output_db_writer(self, doc_type_name):
        if not doc_type_name:
            message = "Could not find anything for the ingestion event ID."
            log(logger, message, use_disclaimer=True)
            return "ingestion_event_id or document_type not found"
        log(logger, f"Found document of type: {doc_type_name}", use_delimiter=False)
        if doc_type_name == DocumentTypeEnum.FINCEN8300.value:
            return OutputDBWriterFincen()
        elif doc_type_name == DocumentTypeEnum.UNKNOWN.value:
            return OutputDBWriterUnknown()
        else:
            return "Invalid document type. Request rejected"