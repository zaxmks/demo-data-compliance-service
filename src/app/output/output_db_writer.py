from src.core.db.db_init import MainDbSession
from src.core.db.models.main_models import EmployeeToComplianceRunEvent, EntityMatchDatum


class OutputDBWriter(object):

    def write_employee_to_compliance_run_event(self, ingestion_event_id, affiliate_filter, row_mapping_config):
        # Write to EmployeeToComplianceRunEvent and add explanation
        with MainDbSession() as main_db:
            for i in range(affiliate_filter.num_records):
                row = affiliate_filter.results_df.iloc[i]
                main_db.add(
                    EmployeeToComplianceRunEvent(
                        employee_id=str(row.employee_id),
                        compliance_run_event_id=ingestion_event_id,
                    )
                )
                match_data = EntityMatchDatum(
                    confidence_threshold=str(
                        row_mapping_config.get_confidence_threshold()
                    ),
                    confidence=str(row.confidence),
                    explanation=str(row.explanation),
                    matched_employee_id=str(row.employee_id),
                    run_event_id=ingestion_event_id,
                )
                main_db.add(match_data)
                main_db.commit()