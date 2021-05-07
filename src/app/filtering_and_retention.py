import json
import logging
from typing import List, Optional

import pandas as pd
from pandas import DataFrame
from requests_futures.sessions import FuturesSession

from kfai_sql_chemistry.db.main import engines

from src.core.db.db_init import MainDbSession, PdfDbSession
from src.core.db.models.main_models import (
    ComplianceRunEvent,
    DocumentType,
    Employee,
    EmployeeToComplianceRunEvent,
    Fincen8300Rev4 as FincenMain,
    EntityMatchDatum,
)
from src.core.db.models.main_models import UnstructuredDocument as UnstructuredDocMain
from src.core.db.models.pdf_models import UnstructuredDocument as UnstructuredDocPdf
from src.core.db.models.pdf_models import (
    Fincen8300Rev4,
    IngestionEvent,
    ParsingStrategyType,
)
from src.core.env.env import ApplicationEnv
from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.sources.data_source import DataSource
from src.app.filtering.unstructured_filter import UnstructuredFilter
from src.app.document_type_enum import DocumentTypeEnum

logger = logging.getLogger(__name__)


class Compliance:
    def __init__(self):
        self._session = FuturesSession()
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
            self.unstructured_column_relations = (
                self._get_unstructured_column_relations()
            )

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
    def _get_unstructured_column_relations() -> List[ColumnRelation]:
        column_relations = [
            ColumnRelation("employee", "ssn", "ssn", 1.0),
            ColumnRelation("employee", "date_of_birth", "date_of_birth", 1.0),
            ColumnRelation("employee", "first_name", "first_name", 1.0),
            ColumnRelation("employee", "last_name", "last_name", 1.0),
        ]
        return column_relations

    @staticmethod
    def _get_employee_data_source() -> Optional[DataSource]:
        engine = engines.get_engine("main")
        with MainDbSession() as session_emp:
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
            df_emp = pd.read_sql(query, engine)
            session_emp.expunge_all()
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

    def _generate_structured_row_matches(self, source: DataSource) -> DataFrame:
        """Generate structured row matches."""
        rows = {
            "first_name": [],  # just for sanity check
            "last_name": [],  # just for sanity check
            "employee_id": [],
            "confidence": [],
            "explanation": [],
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
            rows["first_name"].append(source_row.first_name)
            rows["last_name"].append(source_row.last_name)
            rows["confidence"] = relation.confidence
            rows["explanation"] = relation.match_description
        return DataFrame(rows)

    @staticmethod
    def _get_pdf_document(ingestion_event_id: str) -> pd.DataFrame:
        engine = engines.get_engine("pdf")
        with PdfDbSession() as pdf_session:
            # only reasonable way to get into a dataframe
            query = (
                pdf_session.query(Fincen8300Rev4)
                .filter(Fincen8300Rev4.ingestion_event_id == ingestion_event_id)
                .statement
            )
            df = pd.read_sql(query, engine)
            pdf_session.expunge_all()
            return df

    def _get_ingestion_event_by_id(self, ingestion_event_id: str):
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

    def _get_document_type_by_name(
        self, doc_type_name: DocumentTypeEnum
    ) -> DocumentType:
        with MainDbSession() as main_db:
            doc_type = (
                main_db.query(DocumentType)
                .filter_by(name=doc_type_name.value)
                .one_or_none()
            )
            main_db.expunge_all()
            return doc_type

    def _get_document_type_by_parse_strategy_type(
        self, parse_type: ParsingStrategyType
    ) -> DocumentType:
        with MainDbSession() as main_db:
            if parse_type.name == "configuration_parse":
                doc_type = self._get_document_type_by_name(DocumentTypeEnum.FINCEN8300)
            elif parse_type.name == "unstructured":
                doc_type = self._get_document_type_by_name(DocumentTypeEnum.UNKNOWN)
                logger.info(f"document type {doc_type.name}")
            main_db.expunge_all()
            return doc_type

    def _write_compliance_run_event(
        self, ingestion_event: IngestionEvent, doc_type: DocumentType
    ):
        with MainDbSession() as main_db:
            compliance_run_event = ComplianceRunEvent(
                id=ingestion_event.id,
                s3_bucket=ingestion_event.s3_bucket,
                s3_key=ingestion_event.s3_key,
                was_redacted=False,
                status="ok",
                document_type_id=doc_type.id,
            )
            main_db.add(compliance_run_event)
            main_db.commit()
            main_db.expunge_all()
            logger.info("Finished writing to ComplianceRunEvent")
            return compliance_run_event

    def _get_ingestion_event_and_write_to_compliance(self, ingestion_event_id: str):
        # using the ingestion event id, we grab the pdf data
        ingestion_event = self._get_ingestion_event_by_id(ingestion_event_id)
        with PdfDbSession() as pdf_db:
            parse_type = (
                pdf_db.query(ParsingStrategyType)
                .filter(
                    ParsingStrategyType.id == ingestion_event.parsing_strategy_type_id
                )
                .one_or_none()
            )
        doc_type = self._get_document_type_by_parse_strategy_type(parse_type)
        if not doc_type:
            return None

        self._write_compliance_run_event(ingestion_event, doc_type)
        return doc_type.name

    def filter_and_retain(self, ingestion_event_id: str):
        logger.info(
            "*****************************************************************************"
        )
        logger.info("Begin compliance operations")
        logger.info(
            "*****************************************************************************"
        )
        # Create a class whose job it is to manage querying and paging for employees.
        # Very soon, employees will not be possible to fully fit in memory.
        if self.employee is None:
            logger.info(
                "*****************************************************************************"
            )
            logger.info(
                "No employees in the Main Ingestion DB. Request has been rejected."
            )
            logger.info("In the future, we will: ")
            logger.info("- Remove the ingested PDF data")
            logger.info("- Log removals to the Main Ingestion DB")
            logger.info(
                "*****************************************************************************"
            )
            return "No records in employee table"

        # first get ingestion event
        doc_type_name = self._get_ingestion_event_and_write_to_compliance(
            ingestion_event_id
        )
        if not doc_type_name:
            logger.info(
                "*****************************************************************************"
            )
            logger.info(
                "Could not find anything for the ingestion event ID. "
                "Request has been rejected."
            )
            logger.info("In the future, we will: ")
            logger.info("- Remove the ingested PDF data")
            logger.info("- Log removals to the Main Ingestion DB")
            logger.info(
                "*****************************************************************************"
            )
            return "ingestion_event_id or document_type not found"
        logger.info(f"Found document of type: {doc_type_name}")
        if doc_type_name == DocumentTypeEnum.FINCEN8300.value:
            df = self._get_pdf_document(ingestion_event_id)
            f_vals = df.to_dict(orient="records")[
                0
            ]  # assume one doc per ingestion_event
            ds = DataSource(df)
            ds.column_relations = self.fincen_column_relations
            ds.map_rows_to(
                self.employee, self.value_matching_config, self.row_mapping_config
            )
            results_df = self._generate_structured_row_matches(ds)
            num_records = results_df.shape[0]
        elif doc_type_name == DocumentTypeEnum.UNKNOWN.value:
            unstructured_filter = UnstructuredFilter()
            people_match_df, doc_text = unstructured_filter.filter(ingestion_event_id)
            people_match_df.index = range(people_match_df.shape[0])
            if len(people_match_df) > 0:
                f_vals_list = people_match_df.to_dict(orient="records")
                ds = DataSource(people_match_df)
                ds.column_relations = self.unstructured_column_relations
                ds.map_rows_to(
                    self.employee, self.value_matching_config, self.row_mapping_config
                )
                results_df = self._generate_structured_row_matches(ds)
                num_records = results_df.shape[0]
            else:
                num_records = 0

        else:
            return "Invalid document type. Request rejected"

        if num_records == 0:
            logger.info(
                "*****************************************************************************"
            )
            logger.info("No Entity Matches found. Request rejected.")
            logger.info("In the future, we will: ")
            logger.info("- Remove the ingested PDF data")
            logger.info("- Log removals to the Main Ingestion DB")
            logger.info(
                "*****************************************************************************"
            )
            return "No Entity Matches found. Request rejected."
        else:
            employee_ids = []
            for i in range(num_records):
                row = results_df.iloc[i]
                employee_ids.append(row.employee_id)
                logger.info(
                    "*****************************************************************************"
                )
                logger.info(
                    f"Found match for {row.first_name} {row.last_name} "
                    f"with confidence {row.confidence}, "
                    f"computed by {row.explanation}"
                )
                logger.info(
                    "*****************************************************************************"
                )
        # Write to EmployeeToComplianceRunEvent and add explanation
        with MainDbSession() as main_db:
            for i in range(num_records):
                row = results_df.iloc[i]
                main_db.add(
                    EmployeeToComplianceRunEvent(
                        employee_id=str(row.employee_id),
                        compliance_run_event_id=ingestion_event_id,
                    )
                )
                match_data = EntityMatchDatum(
                    confidence_threshold=str(
                        self.row_mapping_config.get_confidence_threshold()
                    ),
                    confidence=str(row.confidence),
                    explanation=str(row.explanation),
                    matched_employee_id=str(row.employee_id),
                    run_event_id=ingestion_event_id,
                )
                main_db.add(match_data)
                main_db.commit()

        # Write document to main database
        with MainDbSession() as main_db:
            if doc_type_name == DocumentTypeEnum.FINCEN8300.value:
                f_vals["compliance_run_event_id"] = ingestion_event_id
                del f_vals["ingestion_event_id"]
                main_db.add(FincenMain(**f_vals))
            elif doc_type_name == DocumentTypeEnum.UNKNOWN.value:
                # Only write one document and use name of first person
                f_vals = f_vals_list[0]
                # noinspection PyUnboundLocalVariable
                doc = UnstructuredDocMain(
                    first_name=f_vals["first_name"],
                    last_name=f_vals["last_name"],
                    ssn=f_vals["ssn"],
                    date_of_birth=f_vals["date_of_birth"],
                    text=doc_text,
                    compliance_run_event_id=ingestion_event_id,
                )
                main_db.add(doc)
            else:
                return "Invalid document type. Request rejected"
            main_db.commit()

        logger.info(
            "Successfully Migrated PDF data and persisted match information"
            "to the Main Ingestion Database"
        )

        # Remove from pdf database
        with PdfDbSession() as pdf_db:
            if doc_type_name == DocumentTypeEnum.FINCEN8300.value:
                pdf_db.query(Fincen8300Rev4).filter(
                    Fincen8300Rev4.ingestion_event_id == ingestion_event_id
                ).delete()
            elif doc_type_name == DocumentTypeEnum.UNKNOWN.value:
                pdf_db.query(UnstructuredDocPdf).filter(
                    UnstructuredDocPdf.ingestion_event_id == ingestion_event_id
                ).delete()
            else:
                return "Invalid document type. Request rejected"
            pdf_db.commit()

        logger.info(
            "After completion of PDF data migration, delete the file from the db."
        )
        logger.info("Successfully DELETED Employee records from pdf-ingestion-db")

        # Post to rules engine
        headers = {"Content-Type": "application/json"}
        url = ApplicationEnv.RULES_ENGINE_URL()

        if url:
            logger.info(
                "*****************************************************************************"
            )
            logger.info(
                "Prepare API request for rules engine to answer "
                "any possible pathfinder questions for the newly updated Employee."
            )
            logger.info(f"Number of employees matched: {num_records}")
            logger.info(
                "*****************************************************************************"
            )
            self._session.post(
                f"{url}/rules_processor/execute/",
                json={"employeeIdList": employee_ids},
                headers=headers,
            )

        return f"Num documents matched: 1, " f"Num employees matched: {num_records}"
