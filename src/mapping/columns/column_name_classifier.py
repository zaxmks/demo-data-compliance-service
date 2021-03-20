from typing import Dict, Optional
from dataclasses import dataclass

import pandas as pd

from src.mapping.columns.column_label_catalog import ColumnLabelCatalog


@dataclass
class IdInfo:
    table_column_name: str
    table_column_data: pd.Series


class ColumnNameClassifier:
    @classmethod
    def _is_single_column_match(
        cls, label: ColumnLabelCatalog, column_name: str, _column_data: pd.Series
    ) -> bool:
        if label == ColumnLabelCatalog.FULL_NAME:
            return column_name.lower() in ["emp_name", "full_name", "subjectname"]
        elif label == ColumnLabelCatalog.SSN:
            return column_name.lower() in ["ssn", "emp_ssn"]
        elif label == ColumnLabelCatalog.DOB:
            return column_name.lower() in [
                "dateofbirth",
                "date_of_birth",
                "tecs_date_of_birth",
                "subjectdob",
            ]
        elif label == ColumnLabelCatalog.EMAIL:
            return column_name.lower() in [
                "mail",
                "emp_sci_email",
                "email",
                "ssr_email",
            ]
        elif label == ColumnLabelCatalog.FIRST_NAME:
            return column_name.lower() in [
                "first_name",
                "firstname",
                "tecs_first_name",
                "ssr_fn",
            ]
        elif label == ColumnLabelCatalog.MIDDLE_NAME:
            return column_name.lower() in [
                "middle_name",
                "middlename",
                "tecs_middle_name",
                "ssr_mn",
            ]
        elif label == ColumnLabelCatalog.LAST_NAME:
            return column_name.lower() in [
                "last_name",
                "lastname",
                "tecs_last_name",
                "ssr_ln",
            ]
        else:
            raise RuntimeError(f"Invalid label: {label}")

    @classmethod
    def _get_matching_single_column_data(
        cls, label: ColumnLabelCatalog, table_df: pd.DataFrame
    ) -> Optional[IdInfo]:
        for column_name in table_df:
            column_data = table_df[column_name]
            if cls._is_single_column_match(label, column_name, column_data):
                id_info = IdInfo(column_name, column_data)
                return id_info
        return None

    @classmethod
    def get_id_info_from_df(
        cls, table_df: pd.DataFrame
    ) -> Dict[ColumnLabelCatalog, IdInfo]:
        id_to_data: Dict[ColumnLabelCatalog, IdInfo] = {}
        for label in ColumnLabelCatalog:
            id_info = cls._get_matching_single_column_data(label, table_df)
            if id_info:
                id_to_data[label] = id_info
        return id_to_data
