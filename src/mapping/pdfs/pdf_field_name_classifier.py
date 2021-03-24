from typing import Dict, Optional, Any
from dataclasses import dataclass

import pandas as pd

from src.mapping.pdfs.pdf_field_label_catalog import FieldLabelCatalog


@dataclass
class IdInfo:
    field_name: str
    field_data: Any


class FieldNameClassifier:
    @classmethod
    def _is_single_field_match(cls, label: FieldLabelCatalog, field_name: str) -> bool:
        if label == FieldLabelCatalog.FULL_NAME:
            return field_name.lower() in [
                "full_name",
                "name_passenger",
                "namesreported",
                "name",
            ]
        elif label == FieldLabelCatalog.SSN:
            return field_name.lower() in ["tin", "ssn"]
        elif label == FieldLabelCatalog.DOB:
            return field_name.lower() in [
                "dob",
                "date_of_birth_passenger",
                "date_of_birth",
            ]
        elif label == FieldLabelCatalog.FULL_ADDRESS:
            return field_name.lower() in ["address", "full_address"]
        elif label == FieldLabelCatalog.FIRST_NAME:
            return field_name.lower() in ["first_name"]
        elif label == FieldLabelCatalog.LAST_NAME:
            return field_name.lower() in ["last_name", "last_name_entity_name"]
        elif label == FieldLabelCatalog.MIDDLE_NAME:
            return field_name.lower() in ["middle_initial"]
        elif label == FieldLabelCatalog.STREET_ADDRESS:
            return field_name.lower() in ["street_address"]
        elif label == FieldLabelCatalog.CITY:
            return field_name.lower() in ["city"]
        elif label == FieldLabelCatalog.STATE:
            return field_name.lower() in ["state"]
        elif label == FieldLabelCatalog.ZIP:
            return field_name.lower() in ["zip"]
        elif label == FieldLabelCatalog.COUNTRY:
            return field_name.lower() in ["country", "citizenship_country"]
        else:
            raise RuntimeError(f"Invalid label: {label}")

    @classmethod
    def _get_matching_single_field_data(
        cls, label: FieldLabelCatalog, table_df: pd.DataFrame
    ) -> Optional[IdInfo]:
        for field_name in table_df:
            field_data = table_df[field_name]
            if cls._is_single_field_match(label, field_name):
                id_info = IdInfo(field_name, field_data)
                return id_info
        return None

    @classmethod
    def get_id_info_from_df(
        cls, table_df: pd.DataFrame
    ) -> Dict[FieldLabelCatalog, IdInfo]:
        cls.modify_per_field_pseudofield_overlap(table_df)
        id_to_data: Dict[FieldLabelCatalog, IdInfo] = {}
        for label in FieldLabelCatalog:
            id_info = cls._get_matching_single_field_data(label, table_df)
            if id_info:
                id_to_data[label] = id_info
        return id_to_data

    @staticmethod
    def modify_per_field_pseudofield_overlap(df: pd.DataFrame) -> None:
        """
        In one form, "address" means "street address," which is one
        part of "full address;" and in another form "address" means
        "full address." Rename field to remove ambiguity
        """
        if "address" in df and "city" in df:
            df.rename(columns={"address": "street_address"}, inplace=True)
