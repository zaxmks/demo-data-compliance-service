from typing import List, Optional

import json
import requests as API

from pandas import DataFrame
import pandas as pd

from src.sources.data_source import DataSource
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.core.db.config import DatabaseEnum
from src.core.db.models.pdf_models import Fincen8300Rev4
from src.core.db.models.main_models import (
    ComplianceRunEvent,
    EmployeeToComplianceRunEvent,
    Employee,
    DocumentType,
)
from src.core.db.models.main_models import Fincen8300Rev4 as FincenMain
from src.core.db.session import DBContext, DbQuery, AppSession
from src.mapping.columns.column_relation import ColumnRelation
from src.core.db.models.pdf_models import IngestionEvent


class Compliance:
    def __init__(self):
        self.employee = self._get_employee_data_source()

        # If employee is None there is no employee table
        if self.employee:
            value_matching_config_json = self._load_config(
                "config/mapping/levenshtein_default.json"
            )
            row_mapping_config_json = self._load_config(
                "config/mapping/weighted_linear_default.json"
            )
            self.value_matching_config = ValueMatchingConfiguration(
                **value_matching_config_json
            )
            self.row_mapping_config = RowMappingConfiguration(**row_mapping_config_json)
            self.fincen_column_relations = self._get_fincen_column_relations()

    @staticmethod
    def _get_fincen_column_relations() -> List[ColumnRelation]:
        column_relations = [
            ColumnRelation("employee", "tin", "ssn", 1.0),
            ColumnRelation("employee", "dob", "date_of_birth", 1.0),
            ColumnRelation("employee", "first_name", "first_name", 1.0),
            ColumnRelation("employee", "last_name", "last_name", 1.0),
        ]
        return column_relations

    @staticmethod
    def _get_employee_data_source() -> Optional[DataSource]:
        app_emp = AppSession(DatabaseEnum.MAIN_INGESTION_DB)
        session_emp = app_emp.instance
        query = (
            session_emp.query(Employee)
            .with_entities(
                Employee.id,
                Employee.ssn,
                Employee.date_of_birth,
                Employee.first_name,
                Employee.last_name,
            )
            .statement
        )
        df_emp = pd.read_sql(query, app_emp.engine)
        app_emp.instance.close()
        if df_emp.shape[0] == 0:
            employee = None
        else:
            employee = DataSource(df_emp)
        return employee

    @staticmethod
    def _load_config(path) -> dict:
        """
        Read JSON from a filepath
        """
        with open(path, "r") as F:
            return json.load(F)

    def generate_structured_row_matches(self, source: DataSource) -> DataFrame:
        """Generate structured row matches."""
        rows = {
            "first_name": [],  # just for sanity check
            "last_name": [],  # just for sanity check
            "ingestion_event_id": [],
            "employee_id": [],
        }
        # noinspection PyUnresolvedReferences
        for relation in source.row_relations:
            source_index = relation.source_index
            target_index = relation.target_index
            # noinspection PyUnresolvedReferences
            source_row = source.get_data().iloc[source_index]
            # noinspection PyUnresolvedReferences
            target_row = self.employee.get_data().iloc[target_index]
            rows["employee_id"].append(target_row.id)
            rows["ingestion_event_id"].append(source_row.ingestion_event_id)
            rows["first_name"].append(source_row.first_name)
            rows["last_name"].append(source_row.last_name)
        return DataFrame(rows)

    @staticmethod
    def _get_pdf_document(ingestion_event_id: str) -> pd.DataFrame:
        app_pdf = AppSession(DatabaseEnum.PDF_INGESTION_DB)
        session_pdf = app_pdf.instance
        # only reasonable way to get into a dataframe
        query = (
            session_pdf.query(Fincen8300Rev4)
            .filter(Fincen8300Rev4.ingestion_event_id == ingestion_event_id)
            .statement
        )
        df = pd.read_sql(query, app_pdf.engine)
        app_pdf.instance.close()
        return df

    @staticmethod
    def get_ingestion_event_and_write_to_compliance(ingestion_event_id: str):
        with DBContext(DatabaseEnum.PDF_INGESTION_DB) as pdf_db:
            result = (
                pdf_db.query(IngestionEvent)
                .filter(IngestionEvent.id == ingestion_event_id)
                .one_or_none()
            )
            if result is None:
                return None
            with DBContext(DatabaseEnum.MAIN_INGESTION_DB) as main_db:
                doc_type = (
                    main_db.query(DocumentType)
                    .filter(DocumentType.name.like("%fincen%"))
                    .one_or_none()
                )
                if doc_type is None:
                    return None
                main_db.add(
                    ComplianceRunEvent(
                        id=ingestion_event_id,
                        s3_bucket=result.s3_bucket,
                        s3_key=result.s3_key,
                        was_redacted=False,
                        status="ok",
                        document_type_id=doc_type.id,
                    )
                )
        return "done"

    def filter_and_retain(self, ingestion_event_id: str):

        if self.employee is None:
            return "No records in employee table"

        # first get ingestion event
        r = self.get_ingestion_event_and_write_to_compliance(ingestion_event_id)
        if r is None:
            return "ingestion_event_id or document_type not found"
        df = self._get_pdf_document(ingestion_event_id)
        f_vals = df.to_dict(orient="records")[0]  # assume one doc per ingestion_event
        num_document_matches = df.shape[0]
        fincen = DataSource(df)
        fincen.column_relations = self.fincen_column_relations
        fincen.map_rows_to(
            self.employee, self.value_matching_config, self.row_mapping_config
        )
        results_df = self.generate_structured_row_matches(fincen)
        num_records = results_df.shape[0]

        # Write to EmployeeToComplianceRunEvent
        if num_records > 0:
            row = results_df.iloc[0]
            with DBContext(DatabaseEnum.MAIN_INGESTION_DB) as main_db:
                main_db.add(
                    EmployeeToComplianceRunEvent(
                        employee_id=str(row.employee_id),
                        compliance_run_event_id=ingestion_event_id,
                    )
                )

            # Write to Fincen
            del f_vals["ingestion_event_id"]
            with DBContext(DatabaseEnum.MAIN_INGESTION_DB) as main_db:
                main_db.add(FincenMain(**f_vals))

            # Post to rules engine
            API.post(
                f"{ApplicationEnv.RULES_ENGINE_URL()}rules_processor/execute/{[row.employee_id]}"
            )

        return (
            f"Num documents matched: {num_document_matches}, "
            f"Num employees matched: {num_records}"
        )
