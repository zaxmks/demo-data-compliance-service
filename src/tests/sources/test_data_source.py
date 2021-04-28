from unittest.mock import patch

import pandas as pd

from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.ner.ner_configuration import NERConfiguration
from src.sources.data_source import DataSource
from src.sources.structured_data_source import StructuredDataSource
from src.sources.unstructured_data_source import UnstructuredDataSource


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


def test_init_when_structured():
    data = get_sample_directory_df()
    ds = DataSource(data)
    assert isinstance(ds, StructuredDataSource)
    assert list(ds.get_data().values) == list(data.values)
    assert ds.structured
    assert ds.name == "pandas DataFrame (hash 7383002750474244645)"


def test_init_when_unstructured():
    data = "this is an unstructured text string"
    ds = DataSource(data)
    assert isinstance(ds, UnstructuredDataSource)
    assert ds.get_data() == data
    assert not ds.structured
    assert ds.name == "string with hash 9ec30fc91f18445a44b9e9c2820d388d"
