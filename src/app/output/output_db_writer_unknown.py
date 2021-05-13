import logging

from src.app.output.output_db_writer import OutputDBWriter
from src.core.db.db_init import MainDbSession, PdfDbSession
from src.core.db.models.main_models import UnstructuredDocument as UnstructuredDocMain
from src.core.db.models.pdf_models import UnstructuredDocument as UnstructuredDocPdf

logger = logging.getLogger(__name__)

class OutputDBWriterUnknown(OutputDBWriter):

    def write_main_document(self, ingestion_event_id, affiliate_filter):
        with MainDbSession() as main_db:
            # TODO: Inspect this comment below
            # Only write one document and use name of first person
            f_vals = affiliate_filter.f_vals_list[0]
            logger.info('OUTPUT TIME')
            logger.info(f_vals)
            # noinspection PyUnboundLocalVariable
            doc = UnstructuredDocMain(
                first_name=f_vals["first_name"],
                last_name=f_vals["last_name"],
                ssn=f_vals["ssn"],
                date_of_birth=f_vals["date_of_birth"],
                text=affiliate_filter.doc_text,
                compliance_run_event_id=ingestion_event_id,
            )
            main_db.add(doc)

    def delete_pdf_document(self, ingestion_event_id):
        with PdfDbSession() as pdf_db:
            pdf_db.query(UnstructuredDocPdf).filter(
                UnstructuredDocPdf.ingestion_event_id == ingestion_event_id
            ).delete()