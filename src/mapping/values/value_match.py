from dataclasses import dataclass


@dataclass
class ValueMatch:
    target_index: int
    confidence: float
    target_text: str
    source_column: str = None
    target_column: str = None

    def to_dict(self) -> dict:
        """Return dictionary of match contents."""
        return {
            "target_index": self.target_index,
            "target_text": self.target_text,
            "confidence": self.confidence,
            "source_column": self.source_column,
            "target_column": self.target_column,
        }

    def set_source_column(self, source_column_name: str) -> str:
        """Set the source column."""
        self.source_column = source_column_name

    def set_target_column(self, target_column_name: str) -> str:
        """Set the target column name."""
        self.target_column = target_column_name

    def get_target_index(self) -> int:
        """Get the target index value."""
        return self.target_index
