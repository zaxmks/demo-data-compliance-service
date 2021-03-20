from typing import List

from gretel_tools import headers

from src.mapping.values.value_match import ValueMatch
from src.mapping.values.value_matching_target import ValueMatchingTarget


class EmbeddingMatchingModel:
    def __init__(self, embedding_model_type: str = "gretel"):
        """Initialize a new embedding-based matching model."""
        self.similarity_model = self._get_model(embedding_model_type)

    def _get_model(self, embedding_model_type):
        """Get the embedding model to use."""
        if embedding_model_type == "gretel":
            return headers.HeaderAnalyzer()
        else:
            raise NotImplementedError(
                "Embedding similarity model of type %s not currently supported"
                % embedding_model_type
            )

    def predict_single(
        self, src: str, tar: str, matching_target: ValueMatchingTarget
    ) -> float:
        source = matching_target.preprocess_string(src)
        target = matching_target.preprocess_string(tar)
        similarity = self.similarity_model.similarity(source, target)
        return similarity

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
            similarity = self.similarity_model.similarity(source, target)
            if similarity >= confidence_threshold:
                target_index = matching_target.get_target_index(i)
                matches.append(
                    ValueMatch(
                        target_index,
                        similarity,
                        matching_target.get_unprocessed_targets()[target_index],
                    )
                )

        return matches
