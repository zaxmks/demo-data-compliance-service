import logging

from pandas import DataFrame
from requests_futures.sessions import FuturesSession

from src.app.filtering.affiliate_filter_factory import AffiliateFilterFactory
from src.app.input.configuration_loader import ConfigurationLoader
from src.app.input.input_db_reader import InputDBReader
from src.app.output.output_db_writer_factory import OutputDBWriterFactory
from src.core.db.db_init import MainDbSession
from src.core.db.models.main_models import (
    ComplianceRunEvent,
    DocumentType,
)
from src.core.db.models.pdf_models import IngestionEvent

from src.core.env.env import ApplicationEnv
from src.sources.data_source import DataSource

from src.log_util import log

logger = logging.getLogger(__name__)


class Compliance:
    def __init__(self):
        self._session = FuturesSession()
        self.input_reader = InputDBReader()
        self.employee = self.input_reader.get_affiliate_data_source()

        # If employee is None there is no employee table
        if self.employee:
            self.config = ConfigurationLoader("config/mapping/levenshtein_default.json",
                                                     "config/mapping/weighted_linear_default.json")

    def _write_compliance_run_event(
        self, ingestion_event: IngestionEvent):
        with MainDbSession() as main_db:
            compliance_run_event = ComplianceRunEvent(
                id=ingestion_event.id,
                s3_bucket=ingestion_event.s3_bucket,
                s3_key=ingestion_event.s3_key,
                was_redacted=False,
                status="ok",
                document_type_id=ingestion_event.identified_document_type_id,
            )
            main_db.add(compliance_run_event)
            main_db.commit()
            main_db.expunge_all()
            log(logger, "Finished writing to ComplianceRunEvent")
            return compliance_run_event

    def _get_ingestion_event_and_write_to_compliance(self, ingestion_event_id: str):
        # using the ingestion event id, we grab the pdf data
        ingestion_event = self.input_reader.get_ingestion_event_by_id(ingestion_event_id)
        log(logger, "INGESTION EVENT?")
        log(logger, ingestion_event.__dict__)
        doc_type_id = ingestion_event.identified_document_type_id
        if not doc_type_id:
            return None

        self._write_compliance_run_event(ingestion_event)
        return self.input_reader.get_doc_type_name_by_id(doc_type_id)


    def filter_and_retain(self, ingestion_event_id: str):
        log(logger, "Begin compliance operations", use_delimiter=True)
        # Create a class whose job it is to manage querying and paging for employees.
        # Very soon, employees will not be possible to fully fit in memory.
        if self.employee is None:
            message = "No employees in the Main Ingestion DB. Request has been rejected."
            log(logger, message, use_delimiter=True, use_disclaimer=True)
            return "No records in employee table"

        # first get ingestion event
        doc_type_name = self._get_ingestion_event_and_write_to_compliance(
            ingestion_event_id
        )
        if doc_type_name is None:
            message = f"No ingestion_run_event could be found for ingestion_event_id {ingestion_event_id}"
            log(logger, message, use_delimiter=True)
            return message

        affiliate_filter = AffiliateFilterFactory().build_affiliate_filter(doc_type_name, self.employee, self.config)
        affiliate_filter.filter_affiliates(ingestion_event_id, self.config)
        num_records = affiliate_filter.num_records

        if num_records == 0:
            message = "No Entity Matches found. Request rejected."
            log(logger, message, use_delimiter=True, use_disclaimer=True)
            return message
        else:
            employee_ids = affiliate_filter.gather_matches()

        output_db_writer = OutputDBWriterFactory().build_output_db_writer(doc_type_name)
        output_db_writer.write_employee_to_compliance_run_event(ingestion_event_id, affiliate_filter, self.config.row_mapping_config)
        output_db_writer.write_main_document(ingestion_event_id, affiliate_filter)
        message = "Successfully Migrated PDF data and persisted match information"
        message += " to the Main Ingestion Database"
        log(logger, message, use_delimiter=True)

        output_db_writer.delete_pdf_document(ingestion_event_id)
        message = "After completion of PDF data migration, delete the file from the db."
        message += "\nSuccessfully DELETED Employee records from pdf-ingestion-db"
        log(logger, message, use_delimiter=True)

        # Post to rules engine
        headers = {"Content-Type": "application/json"}
        url = ApplicationEnv.RULES_ENGINE_URL()

        if url:
            message = "Prepare API request for rules engine to answer "
            message += "any possible pathfinder questions for the newly updated Employee."
            message += f"Number of employees matched: {num_records}"
            log(logger, message, use_delimiter=True)
            self._session.post(
                f"{url}/rules_processor/execute/",
                json={"employeeIdList": employee_ids},
                headers=headers,
            )

        return f"Num documents matched: 1, " f"Num employees matched: {num_records}"
