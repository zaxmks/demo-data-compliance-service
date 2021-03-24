from typing import List

import editdistance

from src.mapping.values.value_match import ValueMatch
from src.mapping.values.value_matching_target import ValueMatchingTarget


class LevenshteinModel:
    def __init__(self,):
        """Initialize a new levenshtein matching model."""
        pass

    def predict_single(
        self, src: str, tar: str, matching_target: ValueMatchingTarget
    ) -> float:
        source = matching_target.preprocess_string(src)
        target = matching_target.preprocess_string(tar)
        max_len = max(len(source), len(target))
        if max_len == 0:
            # Both source and target have length 0 after preprocessing, assume not a match
            return 0.0
        distance = editdistance.eval(source, target)
        confidence = (1 - (distance / max_len)) ** 2
        return confidence

    def predict(
        self,
        text: str,
        matching_target: ValueMatchingTarget,
        confidence_threshold: float,
    ) -> List[ValueMatch]:
        """Match an entity with a target data source."""
        matches = []
        source = matching_target.preprocess_string(text)
        for i, target in enumerate(matching_target.get_preprocessed_targets()):
            distance = editdistance.eval(source, target)
            max_len = max(len(source), len(target))
            if max_len == 0:
                # Both source and target have length 0 after preprocessing, assume not a match
                continue
            confidence = (1 - (distance / max_len)) ** 2
            if confidence > confidence_threshold:
                target_index = matching_target.get_target_index(i)
                matches.append(
                    ValueMatch(
                        target_index,
                        confidence,
                        matching_target.get_unprocessed_targets()[target_index],
                    )
                )

        return matches
