import pytest

import pandas as pd

from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.rows.row_relation import RowRelation
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.sources.data_source import DataSource
from src.sources.structured_data_source import StructuredDataSource


def get_sample_directory_df():
    df = pd.DataFrame()
    df["name"] = [
        "Michael Connell",
        "Maura Caslin",
        "Steve Jordan",
        "Matthew Whitaker",
        "Bob Hope",
    ]
    return df


def get_test_source():
    data = get_sample_directory_df()
    return StructuredDataSource(data, "test_name")


def test_init():
    sds = get_test_source()
    assert sds.column_relations == []
    assert sds.row_relations == []


def test_append_column_when_correct_length():
    sds = get_test_source()
    sds.append_column(data_to_append=["a", "b", "c", "d", "e"], column_name="letters")
    assert "letters" in sds.data.columns
    assert list(sds.data["letters"].values) == ["a", "b", "c", "d", "e"]


def test_append_column_when_incorrect_length():
    sds = get_test_source()
    with pytest.raises(Exception):
        sds.append_column(data_to_append=["a", "b"], column_name="letters")


def test_append_column_relation_when_correct_type():
    sds = get_test_source()
    cr = ColumnRelation("test_source", "test_source_column", "test_target_column", 1.0)
    sds.append_column_relation(cr)
    assert sds.column_relations == [cr]


def test_append_column_relation_when_incorrect_type():
    sds = get_test_source()
    with pytest.raises(Exception):
        sds.append_column_relation("invalid_type")


def test_append_row_relation_when_correct_type():
    sds = get_test_source()
    rr = RowRelation("test_source", 0, 1, 1.0)
    sds.append_row_relation(rr)
    assert sds.row_relations == [rr]


def test_append_row_relation_when_incorrect_type():
    sds = get_test_source()
    with pytest.raises(Exception):
        sds.append_row_relation("invalid_type")


def test_create_column_relation():
    sds = get_test_source()
    sds.create_column_relation("source_column", "target_column", "target_source")
    assert len(sds.column_relations) == 1
    assert isinstance(sds.column_relations[0], ColumnRelation)
    assert sds.column_relations


def test_get_column_relations():
    sds = get_test_source()
    sds.column_relations = [1, 2, 3]
    assert sds.get_column_relations() == [1, 2, 3]


def test_relate_columns_to():
    ds_source = DataSource("src/tests/test_data/sample/names.csv")
    ds_target = DataSource("src/tests/test_data/sample/names.csv")
    matching_config = ValueMatchingConfiguration(model_type="exact")
    ds_source.relate_columns_to(ds_target, mapping_configuration=matching_config)
    assert len(ds_source.column_relations) == 1
    assert ds_source.column_relations[0].target_data_source == ds_target
    assert ds_source.column_relations[0].source_column_name == "name"
    assert ds_source.column_relations[0].target_column_name == "name"
    assert ds_source.column_relations[0].confidence == 1.0


def test_map_rows_to():
    ds_source = DataSource("src/tests/test_data/sample/names.csv")
    ds_target = DataSource("src/tests/test_data/sample/names.csv")
    ds_source.create_column_relation("name", "name", ds_target)
    value_matching_config = ValueMatchingConfiguration(model_type="exact")
    row_mapping_config = RowMappingConfiguration(
        model_type="weighted_linear", weights={"name": 1}
    )
    ds_source.map_rows_to(ds_target, value_matching_config, row_mapping_config)
    assert len(ds_source.row_relations) == 252  # Duplicate record present, hence +2


def test_describe_row_relation_for_index():
    ds_source = DataSource("src/tests/test_data/sample/names.csv")
    ds_target = DataSource("src/tests/test_data/sample/names.csv")
    ds_source.create_column_relation("name", "name", ds_target)
    description = ds_source.describe_row_relation_for_index(0)
    assert description == '{"name": "Soo Hong"}'


def test_get_column():
    sds = get_test_source()
    ref_df = get_sample_directory_df()
    assert list(sds.get_column("name")) == list(ref_df["name"].values)
