from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnRelation:
    target_data_source: Any
    source_column_name: str
    target_column_name: str
    confidence: float

    def __str__(self) -> str:
        """Build string from relation."""
        return "%s -> %s: %s (%.2f conf)" % (
            self.source_column_name,
            str(self.target_data_source),
            self.target_column_name,
            self.confidence,
        )

    def get_source_column_name(self) -> str:
        """Return the name of the source column."""
        return self.source_column_name

    def get_target_column_name(self) -> str:
        """Return the name of the target column."""
        return self.target_column_name
