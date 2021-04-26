from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ValueMatchDescription:
    value_match_score: float
    normalized_column_weight: float


@dataclass
class RowMatchDescription:
    # str is target field name
    match_dict: Dict[str, ValueMatchDescription]


@dataclass
class RowRelation:
    target_data_source: Any
    source_index: int
    target_index: int
    confidence: float
    match_description: Optional[RowMatchDescription] = None

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
