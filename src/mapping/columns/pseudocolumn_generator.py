import re

import pandas as pd

from src.mapping.columns.column_label_catalog import ColumnLabelCatalog
from src.mapping.columns.column_name_classifier import ColumnNameClassifier


class PseudocolumnGenerator:
    def __init__(self, data_source, concatenation_delimiter: str = " "):
        """Initialize new pseudocolumn generator."""
        self.data_source = data_source
        self.delimiter = concatenation_delimiter
        if not self.data_source.is_structured():
            raise Exception("Can't create pseudocolumns from unstructured data")

    def generate(self):
        """Generate columns based on expected combinations."""
        self._get_canonical_columns()

        # Name groups
        if (
            self._can_generate_full_name()
            and ColumnLabelCatalog.FULL_NAME not in self.canonical_columns
        ):
            self.data_source.append_column(self._generate_full_name(), "full_name")
        elif (
            self._can_generate_first_last_name()
            and ColumnLabelCatalog.FULL_NAME not in self.canonical_columns
        ):
            self.data_source.append_column(
                self._generate_first_last_name(), "full_name"
            )

    def _get_canonical_columns(self):
        """Retrieve canonical column names to concatenate."""
        self.canonical_columns = ColumnNameClassifier.get_id_info_from_df(
            self.data_source.get_data()
        )

    def _can_generate_full_name(self):
        """Check if we can generate a full name from the existing data."""
        return (
            ColumnLabelCatalog.FIRST_NAME in self.canonical_columns
            and ColumnLabelCatalog.MIDDLE_NAME in self.canonical_columns
            and ColumnLabelCatalog.LAST_NAME in self.canonical_columns
        )

    def _can_generate_first_last_name(self):
        """Check if we can generate a full name from the existing data."""
        return (
            ColumnLabelCatalog.FIRST_NAME in self.canonical_columns
            and ColumnLabelCatalog.MIDDLE_NAME not in self.canonical_columns
            and ColumnLabelCatalog.LAST_NAME in self.canonical_columns
        )

    def _generate_full_name(self):
        """Generate a full name column if we have the appropriate input data."""
        first = self.data_source.get_column(
            self.canonical_columns[ColumnLabelCatalog.FIRST_NAME].table_column_name
        )
        middle = self.data_source.get_column(
            self.canonical_columns[ColumnLabelCatalog.MIDDLE_NAME].table_column_name
        )
        last = self.data_source.get_column(
            self.canonical_columns[ColumnLabelCatalog.LAST_NAME].table_column_name
        )
        return [
            self._join_name_pieces([first[i], middle[i], last[i]])
            for i in range(len(first))
        ]

    def _generate_first_last_name(self):
        """Generate a full name column if we have the appropriate input data."""
        first = self.data_source.get_column(
            self.canonical_columns[ColumnLabelCatalog.FIRST_NAME].table_column_name
        )
        last = self.data_source.get_column(
            self.canonical_columns[ColumnLabelCatalog.LAST_NAME].table_column_name
        )
        return [self._join_name_pieces([first[i], last[i]]) for i in range(len(first))]

    def _join_name_pieces(self, name_pieces):
        for i in range(len(name_pieces)):
            if pd.isnull(name_pieces[i]):
                name_pieces[i] = ""
        return self._dedupe_spaces(self.delimiter.join(name_pieces))

    def _dedupe_spaces(self, input_string: str):
        """Convert multiple spaces into a single space."""
        return re.sub(r"\s+", " ", input_string)
