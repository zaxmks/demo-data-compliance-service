import copy
import pytest

import pandas as pd

from src.mapping.columns.column_label_catalog import ColumnLabelCatalog
from src.mapping.columns.pseudocolumn_generator import PseudocolumnGenerator
from src.sources.data_source import DataSource

source_dict = {
    "FIRST_NAME": ["Michael", "Reed", "Jonathan"],
    "MIDDLE_NAME": ["Fred", "George", "Ron"],
    "LAST_NAME": ["McCartney", "Harrison", "Lennon"],
}
full_names = ["Michael Fred McCartney", "Reed George Harrison", "Jonathan Ron Lennon"]
full_names_no_middle = ["Michael McCartney", "Reed Harrison", "Jonathan Lennon"]


def get_source():
    source_df = pd.DataFrame(source_dict)
    return DataSource(source_df)


def get_no_middle_source():
    source_no_middle_dict = copy.deepcopy(source_dict)
    del source_no_middle_dict["MIDDLE_NAME"]
    source_no_middle_df = pd.DataFrame(source_no_middle_dict)
    return DataSource(source_no_middle_df)


def test_init_when_structured_and_default():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    assert pg.data_source == test_source
    assert pg.delimiter == " "


def test_init_when_structured_and_custom_delimiter():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source, concatenation_delimiter=",")
    assert pg.data_source == test_source
    assert pg.delimiter == ","


def test_init_when_unstructured():
    unstructured_source = "this is an unstructured piece of text"
    with pytest.raises(Exception):
        PseudocolumnGenerator(unstructured_source)


def test_generate_when_middle_name_present():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    pg.generate()
    assert "full_name" in test_source.get_data().columns
    assert list(test_source.get_data()["full_name"].values) == full_names


def test_generate_when_middle_name_absent():
    test_source = get_no_middle_source()
    pg = PseudocolumnGenerator(test_source)
    pg.generate()
    assert "full_name" in test_source.get_data().columns
    assert list(test_source.get_data()["full_name"].values) == full_names_no_middle


def test_get_canonical_columns():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    pg._get_canonical_columns()
    assert len(pg.canonical_columns) == 3
    assert ColumnLabelCatalog.FIRST_NAME in pg.canonical_columns
    assert ColumnLabelCatalog.MIDDLE_NAME in pg.canonical_columns
    assert ColumnLabelCatalog.LAST_NAME in pg.canonical_columns


def test_can_generate_full_name_when_true():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    pg._get_canonical_columns()
    assert pg._can_generate_full_name()


def test_can_generate_full_name_when_false():
    test_source = get_no_middle_source()
    pg = PseudocolumnGenerator(test_source)
    pg._get_canonical_columns()
    assert not pg._can_generate_full_name()


def test_can_generate_first_last_name_when_true():
    test_source = get_no_middle_source()
    pg = PseudocolumnGenerator(test_source)
    pg._get_canonical_columns()
    assert pg._can_generate_first_last_name()


def test_can_generate_first_last_name_when_false():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    pg._get_canonical_columns()
    assert not pg._can_generate_first_last_name()


def test_dedupe_spaces():
    test_source = get_source()
    pg = PseudocolumnGenerator(test_source)
    assert pg._dedupe_spaces("This  is a    test") == "This is a test"
