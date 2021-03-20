from src.ner.multi_ner_configuration import MultiNERConfiguration
from src.ner.voting_multi_ner_model import VotingMultiNERModel

class MultiNERModelFactory:
    _model_instances = {}

    @classmethod
    def get_model_from_config(cls, multi_ner_config: MultiNERConfiguration):
        """Return model artifacts, building new ones if need be."""

        model_fingerprint = multi_ner_config.get_fingerprint()
        if model_fingerprint in cls._model_instances:
            return cls._model_instances[model_fingerprint]

        resolution_algorithm = multi_ner_config.get_resolution_algorithm()
        if resolution_algorithm == "simple_voting":
            cls._model_instances[model_fingerprint] = VotingMultiNERModel(multi_ner_config.model_configs)
        else:
            raise NotImplementedError(
                "%s is not a currently supported multi-model type" % resolution_algorithm
        )
        return cls._model_instances[model_fingerprint]