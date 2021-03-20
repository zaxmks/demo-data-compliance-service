from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.values.levenshtein_model import LevenshteinModel
from src.mapping.values.embedding_matching_model import EmbeddingMatchingModel
from src.mapping.values.exact_matching_model import ExactMatchingModel


class ValueMatchingModelFactory:
    _model_instances = {}

    @classmethod
    def get_model_from_config(cls, mapping_config: ValueMatchingConfiguration):
        """Instantiate new matching model.

        Note: 'config' contains any model-specific configurations (must be named).
        """
        model_fingerprint = mapping_config.get_fingerprint()
        if model_fingerprint in cls._model_instances:
            return cls._model_instances[model_fingerprint]

        model_type = mapping_config.get_model_type()
        if model_type == "levenshtein":
            cls._model_instances[model_fingerprint] = LevenshteinModel()
        elif model_type == "exact":
            cls._model_instances[model_fingerprint] = ExactMatchingModel()
        elif model_type == "embedding":
            cls._model_instances[model_fingerprint] = EmbeddingMatchingModel(
                **mapping_config.get_model_config()
            )
        else:
            raise NotImplementedError(
                "%s not currently supported as a matching model type" % model_type
            )
        return cls._model_instances[model_fingerprint]
