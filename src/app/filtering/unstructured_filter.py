import pandas as pd
import logging

from src.core.db.models.pdf_models import UnstructuredDocument
from src.core.db.models.main_models import Employee
from src.core.db.db_init import MainDbSession, PdfDbSession

logger = logging.getLogger(__name__)


class UnstructuredFilter:
    def __init__(self):
        pass

    def filter(self, ingestion_uuid):
        """ Call this when pdf is in unstructured table, outputs (individual_df, raw_text) """
        doc_input = self._read_from_db(ingestion_uuid)
        return self._extract_individuals(doc_input)

    def _read_from_db(self, ingestion_uuid):
        with PdfDbSession() as context:
            print("searching for uuid in unstructured pdf table", ingestion_uuid)
            doc_input = (
                context.query(UnstructuredDocument)
                .filter(UnstructuredDocument.ingestion_event_id == ingestion_uuid)
                .one_or_none()
            )
            context.expunge(doc_input)
            return doc_input

    def _extract_individuals(self, doc_input):
        db_matches = self._get_name_matches(doc_input)
        keys = Employee.__table__.columns.keys()
        individual_df = pd.DataFrame(columns=keys)
        match_types = []

        for employee in db_matches:
            match_dict = {
                "first_name": True,
                "last_name": True,
                "ssn": False,
                "date_of_birth": False,
            }
            if doc_input.ssn and int(employee.ssn) in doc_input.ssn["ssn"]:
                match_dict["ssn"] = True

            if (
                doc_input.dateOfBirth
                and employee.date_of_birth in doc_input.dateOfBirth["date_of_birth"]
            ):
                match_dict["date_of_birth"] = True

            if match_dict["ssn"] or match_dict["date_of_birth"]:
                individual_df = individual_df.append(
                    pd.DataFrame.from_records([self.row2dict(employee)])
                )

            match_types.append(match_dict)

        logger.info(
            f"Matches of these types found in unstructured document: {match_types}"
        )

        return individual_df

    def row2dict(self, row):
        d = {}
        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))

        return d

    def _get_name_matches(self, doc_input):
        with MainDbSession() as context:
            first_name_rows = context.query(Employee).filter(
                Employee.first_name.in_(doc_input.name["name"])
            )
            last_name_rows = (
                context.query(Employee).filter(
                    Employee.last_name.in_(doc_input.name["name"])
                )
            ).subquery()

            result = first_name_rows.join(
                last_name_rows, Employee.id == last_name_rows.columns.id
            ).all()
            context.expunge_all()
            return result
