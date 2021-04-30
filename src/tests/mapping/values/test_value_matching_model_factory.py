import pytest

from src.mapping.values.exact_matching_model import ExactMatchingModel
from src.mapping.values.levenshtein_model import LevenshteinModel
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration


def test_init_when_levenshtein():
    from src.mapping.values.value_matching_model_factory import (
        ValueMatchingModelFactory,
    )

    config = ValueMatchingConfiguration(model_type="levenshtein")
    model = ValueMatchingModelFactory.get_model_from_config(config)
    assert isinstance(model, LevenshteinModel)


def test_init_when_exact():
    from src.mapping.values.value_matching_model_factory import (
        ValueMatchingModelFactory,
    )

    config = ValueMatchingConfiguration(model_type="exact")
    model = ValueMatchingModelFactory.get_model_from_config(config)
    assert isinstance(model, ExactMatchingModel)


def test_init_when_embedding_and_type_invalid():
    from src.mapping.values.value_matching_model_factory import (
        ValueMatchingModelFactory,
    )

    config = ValueMatchingConfiguration(
        model_type="embedding", embedding_model_type="test"
    )
    with pytest.raises(NotImplementedError):
        ValueMatchingModelFactory.get_model_from_config(config)


def test_get_model_from_config_when_called_twice():
    from src.mapping.values.value_matching_model_factory import (
        ValueMatchingModelFactory,
    )

    config = ValueMatchingConfiguration(model_type="exact")
    model1 = ValueMatchingModelFactory.get_model_from_config(config)
    model2 = ValueMatchingModelFactory.get_model_from_config(config)
    assert id(model1) == id(model2)
