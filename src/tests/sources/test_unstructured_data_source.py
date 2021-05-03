import pytest

from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.ner.named_entity import NamedEntity
from src.ner.ner_configuration import NERConfiguration
from src.sources.data_source import DataSource
from src.sources.unstructured_data_source import UnstructuredDataSource


email_text = (
    "This is an example of an email that references Tonya Jones, who is a person."
)


def get_golden_source():
    return DataSource("src/tests/test_data/table/person_base.tsv")


def get_test_source():
    return UnstructuredDataSource(email_text, name="test_email")


def test_init():
    source = get_test_source()
    assert source.data == email_text
    assert not source.structured
    assert source.name == "test_email"
    assert source.entities == []
    assert source.entity_relations == []


def test_relate_entities_to():
    source = get_test_source()
    target = get_golden_source()
    test_entity = NamedEntity("Tonya Jones", 47, 58, "PERSON", 0.9)
    source.entities = [test_entity]
    map_config = ValueMatchingConfiguration(model_type="exact")
    source.relate_entities_to(target, "full_name", map_config, "PERSON")
    assert len(source.entity_relations) == 1
    assert source.entity_relations[0].target_text == "Tonya Jones"
