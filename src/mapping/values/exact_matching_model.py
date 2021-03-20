from typing import List

from metaphone import doublemetaphone

from src.mapping.values.value_match import ValueMatch
from src.mapping.values.value_matching_target import ValueMatchingTarget


class ExactMatchingModel:
    def __init__(self):
        """Initialize a new double metaphone matching model."""
        pass

    def predict_single(
        self, src: str, tar: str, matching_target: ValueMatchingTarget
    ) -> float:
        source = matching_target.preprocess_string(src)
        target = matching_target.preprocess_string(tar)
        if source == target:
            return 1.0
        else:
            return 0.0

    def predict(
        self, text: str, matching_target: ValueMatchingTarget, confidence_threshold
    ) -> List[ValueMatch]:
        """Match an entity with a target data source."""
        matches = []
        source = matching_target.preprocess_string(text)
        for i, target in enumerate(matching_target.get_preprocessed_targets()):
            if source == target:
                if confidence_threshold <= 1.0:
                    target_index = matching_target.get_target_index(i)
                    matches.append(
                        ValueMatch(
                            target_index,
                            1.0,
                            matching_target.get_unprocessed_targets()[target_index],
                        )
                    )

        return matches
