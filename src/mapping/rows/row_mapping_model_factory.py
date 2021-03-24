from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.rows.weighted_linear_model import WeightedLinearModel


class RowMappingModelFactory:
    _model_instances = {}

    @classmethod
    def get_model_from_config(cls, mapping_config: RowMappingConfiguration):
        """Instantiate a new row mapping model."""
        model_fingerprint = mapping_config.get_fingerprint()
        if model_fingerprint in cls._model_instances:
            return cls._model_instances[model_fingerprint]

        model_type = mapping_config.get_model_type()
        if model_type == "weighted_linear":
            cls._model_instances[model_fingerprint] = WeightedLinearModel(
                **mapping_config.get_model_config()
            )
            return cls._model_instances[model_fingerprint]
        else:
            raise NotImplementedError(
                "%s not currently supported as a matching model type" % model_type
            )
