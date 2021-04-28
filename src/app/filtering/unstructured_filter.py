import pandas as pd
import logging
import json

from src.core.db.models.pdf_models import UnstructuredDocument
from src.core.db.models.main_models import Employee
from src.core.db.db_init import MainDbSession, PdfDbSession
from src.app.filtering.unstructured_match import UnstructuredMatch

logger = logging.getLogger(__name__)


class UnstructuredFilter:
    def __init__(self):
        pass

    def filter(self, ingestion_uuid):
        """ Call this when pdf is in unstructured table, outputs (individual_df, raw_text) """
        doc_input = self._read_from_db(ingestion_uuid)
        return self._extract_individuals(doc_input), doc_input.text

    def _read_from_db(self, ingestion_uuid):
        with PdfDbSession() as context:
            print("searching for uuid in unstructured pdf table", ingestion_uuid)
            doc_input = (
                context.query(UnstructuredDocument)
                .filter(UnstructuredDocument.ingestion_event_id == ingestion_uuid)
                .one_or_none()
            )
            self._reformat_doc_input(doc_input)

            context.expunge(doc_input)
            return doc_input

    def _reformat_doc_input(self, doc_input: UnstructuredDocument):
        doc_input.name = json.loads(doc_input.name)
        doc_input.dateOfBirth = json.loads(doc_input.dateOfBirth)
        doc_input.ssn = json.loads(doc_input.ssn)
        doc_input.zipCode = json.loads(doc_input.zipCode)

    def _extract_individuals(self, doc_input):
        db_matches = self._get_name_matches(doc_input)
        keys = Employee.__table__.columns.keys()
        individual_df = pd.DataFrame(columns=keys)
        match_types = []

        for employee in db_matches:
            match = UnstructuredMatch()
            if doc_input.ssn and int(employee.ssn) in doc_input.ssn:
                match.set_true(match.SSN)

            if (
                doc_input.dateOfBirth
                and employee.date_of_birth in doc_input.dateOfBirth
            ):
                match.set_true(match.DATE_OF_BIRTH)

            if match.is_match(match.SSN) or match.is_match(match.DATE_OF_BIRTH):
                individual_df = individual_df.append(
                    pd.DataFrame.from_records([self.row2dict(employee)])
                )

            match_types.append(match)

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
        """
        Get rows that have name in Employee.first_name, do the same for Employee.last_name, then join on id
        """
        with MainDbSession() as context:
            first_name_rows = context.query(Employee).filter(
                Employee.first_name.in_(doc_input.name)
            )
            last_name_rows = (
                context.query(Employee).filter(Employee.last_name.in_(doc_input.name))
            ).subquery()

            result = first_name_rows.join(
                last_name_rows, Employee.id == last_name_rows.columns.id
            ).all()
            context.expunge_all()
            return result
