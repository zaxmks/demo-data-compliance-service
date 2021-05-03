from unittest.mock import patch

from src.sources.data_source_base import DataSourceBase


def get_test_source():
    return DataSourceBase(data="test_data", structured=True, name="test_name")


def test_init():
    dsb = get_test_source()
    assert dsb.data == "test_data"
    assert dsb.structured
    assert dsb.name == "test_name"


def test_reserved_methods():
    dsb = get_test_source()
    assert str(dsb) == "test_name"
    assert repr(dsb) == "test_name"
    assert len(dsb) == 9


def test_is_structured():
    dsb = get_test_source()
    assert dsb.is_structured()


def test_get_data():
    dsb = get_test_source()
    assert dsb.get_data() == "test_data"
