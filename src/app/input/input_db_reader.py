from typing import Optional
import logging

import pandas as pd

from kfai_sql_chemistry.db.main import engines

from src.core.db.db_init import MainDbSession, PdfDbSession

from src.core.db.models.pdf_models import UnstructuredDocument as UnstructuredDocPdf, DocumentType
from src.core.db.models.pdf_models import (
    Fincen8300Rev4,
    IngestionEvent,
    ParsingStrategyType,
)
from src.core.db.models.main_models import Employee
from src.log_util import log
from src.sources.data_source import DataSource

logger = logging.getLogger(__name__)

class InputDBReader(object):

    @staticmethod
    def get_affiliate_data_source() -> Optional[DataSource]:
        engine = engines.get_engine("main")
        with MainDbSession() as session_aff:
            query = (
                session_aff.query(Employee)
                .with_entities(
                    Employee.id,
                    Employee.ssn,
                    Employee.date_of_birth,
                    Employee.first_name,
                    Employee.last_name,
                )
                .statement
            )
            df_aff = pd.read_sql(query, engine)
            session_aff.expunge_all()
            if df_aff.shape[0] == 0:
                affiliate = None
            else:
                affiliate = DataSource(df_aff)
            return affiliate

    @staticmethod
    def _get_pdf_document(ingestion_event_id: str) -> pd.DataFrame:
        engine = engines.get_engine("pdf")
        with PdfDbSession() as pdf_session:
            # only reasonable way to get into a dataframe
            # TODO: hardcoded for fincen??
            query = (
                pdf_session.query(Fincen8300Rev4)
                .filter(Fincen8300Rev4.ingestion_event_id == ingestion_event_id)
                .statement
            )
            df = pd.read_sql(query, engine)
            pdf_session.expunge_all()
            return df

    def get_ingestion_event_by_id(self, ingestion_event_id: str):
        with PdfDbSession() as pdf_db:
            result = (
                pdf_db.query(IngestionEvent)
                .filter(IngestionEvent.id == ingestion_event_id)
                .one_or_none()
            )
            if not result:
                return None
            pdf_db.expunge_all()
            return result

    def get_doc_type_name_by_id(self, doc_type_id: str):
        log(logger, "GETTING DOCUMENT NAME")
        log(logger, doc_type_id, use_delimiter=True)
        with PdfDbSession() as pdf_db:
            result = (
                pdf_db.query(DocumentType)
                .filter(DocumentType.id == doc_type_id)
                .one_or_none()
            )
            if not result:
                return None
            pdf_db.expunge_all()
            return result.name
