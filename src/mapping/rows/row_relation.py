from dataclasses import dataclass
from typing import Any


@dataclass
class RowRelation:
    target_data_source: Any
    source_index: int
    target_index: int
    confidence: float

    def __str__(self) -> str:
        """Build string from relation."""
        return "idx %s -> %s: idx %s (%.2f conf)" % (
            str(self.source_index),
            str(self.target_data_source),
            str(self.target_index),
            self.confidence,
        )

    def to_dict(self) -> dict:
        return {
            "source_index": self.source_index,
            "target_index": self.target_index,
            "confidence": self.confidence,
        }
