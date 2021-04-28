import pytest

import pandas as pd

from src.clients.s3_client import S3Client
from src.sources.data_loader import DataLoader


def test_init():
    dl = DataLoader("test_source", "test_client")
    assert dl.data_source == "test_source"
    assert dl.client == "test_client"


# def test_load_when_database_client():
#     db_client = DatabaseClient(temp=True)
#     db_client.connect()
#     db_client.execute("CREATE TABLE test_table (name TEXT)")
#     db_client.execute("INSERT INTO test_table(name) VALUES ('test_name')")
#     dl = DataLoader("test_table", client=db_client)
#     data, structured, name = dl.load()
#     assert isinstance(data, pd.DataFrame)
#     assert data.columns == "name"
#     assert data.values == ["test_name"]
#     assert structured
#     assert name == "test_table"
#
#
# def test_load_when_s3_file_specified():
#     with pytest.raises(NotImplementedError):
#         dl = DataLoader("test", S3Client())
#         dl.load()


def test_load_when_csv_file_specified():
    dl = DataLoader("src/tests/test_data/sample/names.csv", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, pd.DataFrame)
    assert data.columns == ["name"]
    assert len(data.values) == 250
    assert structured
    assert name == "src/tests/test_data/sample/names.csv"


def test_load_when_dataframe_specified():
    test_df = pd.read_csv("src/tests/test_data/sample/names.csv")
    dl = DataLoader(test_df, client=None)
    data, structured, name = dl.load()
    assert isinstance(data, pd.DataFrame)
    assert data.columns == ["name"]
    assert len(data.values) == 250
    assert structured
    assert name == "pandas DataFrame (hash 5214317343533855748)"


def test_load_when_txt_file_specified():
    dl = DataLoader("src/tests/test_data/sample/email.txt", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, str)
    assert data.startswith("Dear Mr. Connell")
    assert not structured
    assert name == "src/tests/test_data/sample/email.txt"


def test_load_when_pdf_file_specified():
    dl = DataLoader("src/tests/test_data/sample/academic_paper.pdf", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, str)
    assert data.startswith("Enriching Word Vectors")
    assert not structured
    assert name == "src/tests/test_data/sample/academic_paper.pdf"


def test_load_when_xml_file_specified():
    dl = DataLoader("src/tests/test_data/sample/employees.xml", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 6
    assert len(data.columns) == 9
    assert structured
    assert name == "src/tests/test_data/sample/employees.xml"


def test_load_when_xls_file_specified():
    dl = DataLoader("src/tests/test_data/sample/dummy.xls", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 4
    assert len(data.columns) == 3
    assert structured
    assert name == "src/tests/test_data/sample/dummy.xls"


def test_load_when_xlsx_file_specified():
    dl = DataLoader("src/tests/test_data/sample/dummy.xlsx", client=None)
    data, structured, name = dl.load()
    assert isinstance(data, pd.DataFrame)
    assert len(data) == 4
    assert len(data.columns) == 3
    assert structured
    assert name == "src/tests/test_data/sample/dummy.xlsx"


def test_load_when_multi_sheet_xlsx():
    dl = DataLoader("src/tests/test_data/sample/dummy_two_sheets.xlsx", client=None)
    with pytest.raises(NotImplementedError):
        dl.load()
