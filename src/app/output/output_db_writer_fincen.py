from src.app.output.output_db_writer import OutputDBWriter
from src.core.db.db_init import MainDbSession, PdfDbSession

from src.core.db.models.main_models import Fincen8300Rev4 as FincenMain
from src.core.db.models.pdf_models import Fincen8300Rev4 as FincenPDF

class OutputDBWriterFincen(OutputDBWriter):

    def write_main_document(self, ingestion_event_id, affiliate_filter):
        with MainDbSession() as main_db:
            affiliate_filter.f_vals["compliance_run_event_id"] = ingestion_event_id
            del affiliate_filter.f_vals["ingestion_event_id"]
            main_db.add(FincenMain(**affiliate_filter.f_vals))

    def delete_pdf_document(self, ingestion_event_id):
        with PdfDbSession() as pdf_db:
            pdf_db.query(FincenPDF).filter(
                FincenPDF.ingestion_event_id == ingestion_event_id
            ).delete()