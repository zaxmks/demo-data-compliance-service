from typing import List

import json

from pandas import DataFrame
import pandas as pd

from src.sources.data_source import DataSource

from src.mapping.pdfs.pdf_field_name_classifier import FieldNameClassifier
from src.mapping.pdfs.pdf_field_label_catalog import FieldLabelCatalog
from src.mapping.pdfs.pseudofield_generator import PseudofieldGenerator
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.core.db.config import DatabaseEnum
from src.core.db.models.pdf_models import Fincen8300Rev4
from src.core.db.models.main_models import EmployeeToDocument
from src.core.db.session import DBContext, DbQuery, AppSession
from src.mapping.columns.column_relation import ColumnRelation


class ColumnMap:
    def __init__(self):
        self.employee = self._get_employee_data_source()
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

    def _get_fincen_column_relations(self) -> List[ColumnRelation]:
        fincen = self._get_fincen_data_source()

        pseudofield_generator = PseudofieldGenerator(fincen)
        pseudofield_generator.generate()

        # noinspection PyTypeChecker
        self._create_column_relations_for(fincen, self.employee)
        # noinspection PyTypeChecker
        return fincen.column_relations

    @staticmethod
    def _get_fincen_data_source() -> DataSource:
        db = DbQuery(DatabaseEnum.PDF_INGESTION_DB)
        result = db.execute("SELECT * from public.fincen8300_rev4 limit 10")
        df = DataFrame(result.fetchall())
        df.columns = result.keys()
        fincen = DataSource(df)
        return fincen

    @staticmethod
    def _get_employee_data_source() -> DataSource:
        db = DbQuery(DatabaseEnum.MAIN_INGESTION_DB)
        result = db.execute("SELECT * from public.employee")
        df = DataFrame(result.fetchall())
        df.columns = result.keys()
        employee = DataSource(df)
        return employee

    @staticmethod
    def _create_column_relations_for(source, target):
        """Create column relations from canonical column identifiers."""
        gold_id_info = FieldNameClassifier.get_id_info_from_df(target.get_data())
        data_id_info = FieldNameClassifier.get_id_info_from_df(source.get_data())
        for identifier in FieldLabelCatalog:
            if identifier in gold_id_info and identifier in data_id_info:
                g_id = gold_id_info[identifier]
                d_id = data_id_info[identifier]
                source.create_column_relation(d_id.field_name, g_id.field_name, target)
                print("New relation detected: %s" % str(source.column_relations[-1]))

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


cm = ColumnMap()


def filter_and_retain(ingestion_event_id: str):
    app_pdf = AppSession(DatabaseEnum.PDF_INGESTION_DB)
    session_pdf = app_pdf.instance
    # only reasonable way to get into a dataframe
    query = (
        session_pdf.query(Fincen8300Rev4)
        .filter(Fincen8300Rev4.ingestion_event_id == ingestion_event_id)
        .statement
    )
    df = pd.read_sql(query, app_pdf.engine)
    num_document_matches = df.shape[0]
    fincen = DataSource(df)
    fincen.column_relations = cm.fincen_column_relations
    fincen.map_rows_to(cm.employee, cm.value_matching_config, cm.row_mapping_config)
    results_df = cm.generate_structured_row_matches(fincen)
    num_records = results_df.shape[0]
    with DBContext(DatabaseEnum.MAIN_INGESTION_DB) as main_db:
        for i in range(num_records):
            row = results_df.iloc[i]
            main_db.add(
                EmployeeToDocument(
                    employee_id=str(row.employee_id),
                    ingestion_event_id=str(row.ingestion_event_id),
                )
            )
    return (
        f"Num documents matched: {num_document_matches}, "
        f"Num employees matched: {num_records}"
    )
