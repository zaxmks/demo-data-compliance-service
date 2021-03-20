from src.ner.ner_configuration import NERConfiguration
from src.ner.hugging_face_ner_model import HuggingFaceNERModel
from src.ner.spacy_model import SpacyModel


class NERModelFactory:
    _model_instances = {}

    @classmethod
    def get_model_from_config(cls, ner_config: NERConfiguration):
        """Return model artifact, building a new one if need be."""
        model_fingerprint = ner_config.get_fingerprint()
        if model_fingerprint in cls._model_instances:
            return cls._model_instances[model_fingerprint]

        model_type = ner_config.get_model_type()
        if model_type == "spacy_en_core_web_sm":
            cls._model_instances[model_fingerprint] = SpacyModel("en_core_web_sm")
        elif model_type == "spacy_en_core_web_lg":
            cls._model_instances[model_fingerprint] = SpacyModel("en_core_web_lg")
        elif model_type == "hugging_face":
            cls._model_instances[model_fingerprint] = HuggingFaceNERModel()
        else:
            raise NotImplementedError(
                "%s is not a currently supported model type" % model_type
            )
        return cls._model_instances[model_fingerprint]
