import pandas as pd

from src.mapping.columns.column_label_catalog import ColumnLabelCatalog
from src.mapping.columns.column_name_classifier import ColumnNameClassifier


def test_full_name():
    data = {
        "full_name": ["Bob Johnson", "Sally Sallyson", "Aniket Prada"],
        "ssn": ["444-44-4444", "555-55-5555", "777-77-7777"],
        "other": ["a", "b", "c"],
    }
    df = pd.DataFrame(data)
    print(df)
    id_info = ColumnNameClassifier.get_id_info_from_df(df)
    print(id_info)
    assert len(id_info) == 2
    assert ColumnLabelCatalog.FULL_NAME in id_info
    assert ColumnLabelCatalog.SSN in id_info


def test_name_parts():
    data = {
        "first_name": ["Bob", "Sally", "Aniket"],
        "middle_name": ["aaa", "bbb", "ccc"],
        "last_name": ["Johnson", "Sallyson", "Prada"],
        "ssn": ["444-44-4444", "555-55-5555", "777-77-7777"],
        "other": ["a", "b", "c"],
    }
    df = pd.DataFrame(data)
    print("Separate columns for names")
    id_info = ColumnNameClassifier.get_id_info_from_df(df)
    print(id_info)
    assert len(id_info) == 4
    assert id_info[ColumnLabelCatalog.FIRST_NAME].table_column_name == "first_name"
