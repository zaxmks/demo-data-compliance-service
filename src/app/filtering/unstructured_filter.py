import pandas as pd
import logging
import ast
from datetime import datetime

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
        logger.info("Resolving unstructured parser doc input")
        doc_input = self._read_from_db(ingestion_uuid)
        return self._extract_individuals(doc_input), doc_input.text

    def _read_from_db(self, ingestion_uuid):
        with PdfDbSession() as context:
            doc_input = (
                context.query(UnstructuredDocument)
                .filter(UnstructuredDocument.ingestion_event_id == ingestion_uuid)
                .one_or_none()
            )
            context.expunge(doc_input)
            self._convert_doc_input(doc_input)
            return doc_input

    def _convert_doc_input(self, doc_input):
        doc_input.name = (
            ast.literal_eval(str(doc_input.name)) if doc_input.name else None
        )
        doc_input.ssn = ast.literal_eval(str(doc_input.ssn)) if doc_input.ssn else None
        doc_input.dateOfBirth = (
            ast.literal_eval(str(doc_input.dateOfBirth))
            if doc_input.dateOfBirth
            else None
        )
        doc_input.zipCode = (
            ast.literal_eval(str(doc_input.zipCode)) if doc_input.zipCode else None
        )

        self._format_ssn(doc_input)

    def _format_ssn(self, doc_input):
        if doc_input.ssn:
            doc_input.ssn = [str(item) for item in doc_input.ssn]

    def _format_dob(self, doc_input):
        doc_input.dateOfBirth = [
            datetime.strptime(item, "%m/%d/%Y").date() for item in doc_input.dateOfBirth
        ]

    def _match_input_to_employee(self, doc_input, employee, match):
        # Match ssn if ssn from docinput matches emplyee record OR join happened on ssn
        if (
            doc_input.ssn and str(employee.ssn) in doc_input.ssn
        ) or match.match_flag == match.SSN:
            match.set_true(match.SSN)

        # Match first_name if first_name from docinput matches emplyee record OR join happened on first_name AND last_name
        if (
            (doc_input.name and employee.first_name in doc_input.name)
            and match.match_flag == match.SSN
        ) or match.match_flag == match.FULL_NAME:
            match.set_true(match.FIRST_NAME)

        # Match last_name if last_name from docinput matches emplyee record OR join happened on first_name AND last_name
        if (
            (doc_input.name and employee.last_name in doc_input.name)
            and match.match_flag == match.SSN
        ) or match.match_flag == match.FULL_NAME:
            match.set_true(match.LAST_NAME)

        # Match dob if dob in employee record
        if doc_input.dateOfBirth and self._date_in_list(
            datetime.strftime(employee.date_of_birth, "%m/%d/%Y"),
            doc_input.dateOfBirth,
        ):
            match.set_true(match.DATE_OF_BIRTH)

    def _extract_individuals(self, doc_input):
        match_flag = ""
        if doc_input.ssn and len(doc_input.ssn) > 0:
            db_matches = self._get_snn_matches(doc_input)
            match_flag = UnstructuredMatch.SSN
        else:
            db_matches = self._get_name_matches(doc_input)
            match_flag = UnstructuredMatch.FULL_NAME

        keys = Employee.__table__.columns.keys()
        individual_df = pd.DataFrame(columns=keys)
        match_types = []

        logger.info(
            f"Unstructured filter found {len(db_matches)} (potential) unique employees mentioned in document"
        )

        for employee in db_matches:
            match = UnstructuredMatch(match_flag)
            self._match_input_to_employee(doc_input, employee, match)

            # Build match dict and convert to df
            if (
                (match.is_match(match.FIRST_NAME) and match.is_match(match.LAST_NAME))
                or match.is_match(match.SSN)
                or match.is_match(match.DATE_OF_BIRTH)
            ):
                employee_dict = employee.__dict__
                valid_employee_dict = {}
                for key, val in employee_dict.items():
                    if match.is_match(key):
                        valid_employee_dict[key] = val
                new_df = pd.DataFrame.from_records([valid_employee_dict])
                individual_df = individual_df.append(new_df)
            match_types.append(match)

        individual_df = individual_df.where(pd.notnull(individual_df), None)

        logger.info(
            f"Matches of these types found in unstructured document: {match_types}"
        )

        return individual_df

    def _date_in_list(self, item, lst):
        for datestr in lst:
            logger.info(f"option: {datestr}, truth: {item}")
            if datestr == item:
                return True
        return False

    def _get_snn_matches(self, doc_input):
        """
        Get rows that have an ssn in doc_input.ssn
        """
        with MainDbSession() as context:
            result = (
                context.query(Employee).filter(Employee.ssn.in_(doc_input.ssn)).all()
            )
            context.expunge_all()
            return result

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
