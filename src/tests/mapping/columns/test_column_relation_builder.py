import pytest
from unittest.mock import Mock

from src.mapping.columns.column_relation_builder import ColumnRelationBuilder
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.values.value_match import ValueMatch
from src.sources.data_source import DataSource

test_match = ValueMatch(target_index=1234, confidence=0.1234, target_text="TEST")
test_mapping_config = ValueMatchingConfiguration(
    map_by="name",
    confidence_threshold=0.5,
    model_type="exact",
    ignore_case=True,
    ignore_special_characters=True,
    ignore_digits=True,
)


def test_init_when_source_and_target_structured():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/employees.xml")
    crb = ColumnRelationBuilder(source, target)
    assert crb.source == source
    assert crb.target == target


def test_init_when_source_unstructured():
    source = DataSource("src/tests/test_data/sample/email.txt")
    target = DataSource("src/tests/test_data/sample/names.csv")
    with pytest.raises(TypeError):
        crm = ColumnRelationBuilder(source, target)


def test_init_when_target_unstructured():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/email.txt")
    with pytest.raises(TypeError):
        crm = ColumnRelationBuilder(source, target)


def test_build_relations_from_matches_when_under_threshold():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/employees.xml")
    crm = ColumnRelationBuilder(source, target)
    assert crm._build_relations_from_matches("SOURCETEST", [test_match], 0.5) == []


def test_build_relations_from_matches_when_above_threshold():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/employees.xml")
    crm = ColumnRelationBuilder(source, target)
    relations = crm._build_relations_from_matches("SOURCETEST", [test_match], 0.1)
    assert len(relations) == 1
    assert relations[0].target_data_source == target
    assert relations[0].source_column_name == "SOURCETEST"
    assert relations[0].target_column_name == "TEST"
    assert relations[0].confidence == 0.1234


def test_get_relations_when_map_by_name():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/employees.xml")
    crm = ColumnRelationBuilder(source, target)
    mapping_configuration = Mock()
    mapping_configuration.get_map_by_type.return_value = "name"
    crm._get_relations_by_name = Mock()
    crm.get_relations(mapping_configuration)
    assert mapping_configuration.get_map_by_type.call_count == 1
    assert crm._get_relations_by_name.call_count == 1
    crm._get_relations_by_name.assert_called_with(mapping_configuration)


def test_get_relations_when_not_map_by_name():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/employees.xml")
    crm = ColumnRelationBuilder(source, target)
    mapping_configuration = Mock()
    mapping_configuration.get_map_by_type.return_value = "test"
    with pytest.raises(NotImplementedError):
        crm.get_relations(mapping_configuration)


def test_get_relations_by_name():
    source = DataSource("src/tests/test_data/sample/names.csv")
    target = DataSource("src/tests/test_data/sample/names.csv")
    crm = ColumnRelationBuilder(source, target)
    relations = crm._get_relations_by_name(test_mapping_config)
    print(relations)
    assert len(relations) == 1
    assert relations[0].target_data_source == target
    assert relations[0].source_column_name == "name"
    assert relations[0].target_column_name == "name"
    assert relations[0].confidence == 1.0
