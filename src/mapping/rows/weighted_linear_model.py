import logging
from typing import List

from src.mapping.values.value_match import ValueMatch
from src.mapping.columns.column_relation import ColumnRelation

logger = logging.getLogger(__name__)


class WeightedLinearModel:
    def __init__(self, weights=None, null_confidence=1.0):
        self.weights = weights
        self.null_confidence = null_confidence

    def predict(
        self,
        column_relations: List[ColumnRelation],
        value_match_group: List[ValueMatch],
    ) -> float:
        """Predict confidence from a list of value matches."""
        # Remove weights for columns that aren't present in the source data (i.e. don't penalized matches because the .csv didn't have ssn)
        matched_columns = {cr.target_column_name for cr in column_relations}
        weighted_columns = matched_columns & self.weights.keys()
        total_weight = sum([self.weights[w] for w in weighted_columns])
        weights = {c: self.weights[c] / total_weight for c in weighted_columns}
        dimensions = {c: self.null_confidence for c in weighted_columns}
        for vm in value_match_group:
            if vm.target_column in dimensions:
                dimensions[vm.target_column] = vm.confidence
        activations = {c: dimensions[c] * weights[c] for c in weighted_columns}
        return sum(activations.values())
