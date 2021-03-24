from typing import List

import re

import pandas as pd

from src.mapping.pdfs.pdf_field_label_catalog import FieldLabelCatalog
from src.mapping.pdfs.pdf_field_name_classifier import FieldNameClassifier


class PseudofieldGenerator:
    def __init__(self, data_source, concatenation_delimiter: str = " "):
        """Initialize new pseudofield generator."""
        self.data_source = data_source
        self.delimiter = concatenation_delimiter
        if not self.data_source.is_structured():
            raise Exception("Can't create pseudofields from unstructured data")

    def generate(self):
        """Generate fields based on expected combinations."""
        self._get_canonical_fields()

        # Name groups
        if (
            self._can_generate_full_name()
            and FieldLabelCatalog.FULL_NAME not in self.canonical_fields
        ):
            self.data_source.append_column(self._generate_full_name(), "full_name")

        if self._can_generate_full_address():
            fields = [
                FieldLabelCatalog.STREET_ADDRESS,
                FieldLabelCatalog.CITY,
                FieldLabelCatalog.STATE,
                FieldLabelCatalog.ZIP,
                FieldLabelCatalog.COUNTRY,
            ]
            field_data = self._generate_full_field(fields)
            self.data_source.append_column(field_data, "full_address")

    def _get_canonical_fields(self):
        """Retrieve canonical field names to concatenate."""
        self.canonical_fields = FieldNameClassifier.get_id_info_from_df(
            self.data_source.get_data()
        )

    def _can_generate_full_name(self):
        """Check if we can generate a full name from the existing data."""
        return (
            FieldLabelCatalog.FIRST_NAME in self.canonical_fields
            and FieldLabelCatalog.MIDDLE_NAME in self.canonical_fields
            and FieldLabelCatalog.LAST_NAME in self.canonical_fields
        )

    def _can_generate_full_address(self):
        """Check if we can generate a full address from the existing data."""
        return (
            FieldLabelCatalog.STREET_ADDRESS in self.canonical_fields
            and FieldLabelCatalog.CITY in self.canonical_fields
            and FieldLabelCatalog.STATE in self.canonical_fields
            and FieldLabelCatalog.ZIP in self.canonical_fields
            and FieldLabelCatalog.COUNTRY in self.canonical_fields
        )

    def _generate_full_name(self):
        """Generate a full name field if we have the appropriate input data."""
        first = self.data_source.get_column(
            self.canonical_fields[FieldLabelCatalog.FIRST_NAME].field_name
        )
        middle = self.data_source.get_column(
            self.canonical_fields[FieldLabelCatalog.MIDDLE_NAME].field_name
        )
        last = self.data_source.get_column(
            self.canonical_fields[FieldLabelCatalog.LAST_NAME].field_name
        )
        return [
            self._join_name_pieces([first[i], middle[i], last[i]])
            for i in range(len(first))
        ]

    def _generate_full_field(self, fields: List[FieldLabelCatalog]) -> List[str]:
        """Generate a full field if we have the appropriate input data."""
        data_fields: List[pd.Series] = []
        for field in fields:
            field_name = self.canonical_fields[field].field_name
            data_fields.append(self.data_source.get_column(field_name))

        def get_vals(i: int) -> List[str]:
            return [v[i] for v in data_fields]

        num_recs = len(data_fields[0])
        return [self._join_name_pieces(get_vals(i)) for i in range(num_recs)]

    def _join_name_pieces(self, name_pieces) -> str:
        for i in range(len(name_pieces)):
            if pd.isnull(name_pieces[i]):
                name_pieces[i] = ""
        return self._dedupe_spaces(self.delimiter.join(name_pieces))

    @staticmethod
    def _dedupe_spaces(input_string: str) -> str:
        """Convert multiple spaces into a single space."""
        return re.sub(r"\s+", " ", input_string)
