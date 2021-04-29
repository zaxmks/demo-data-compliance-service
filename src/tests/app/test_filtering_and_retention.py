from unittest.mock import Mock, patch
from requests_futures.sessions import FuturesSession

from src.app.filtering_and_retention import Compliance
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration

@patch("src.app.filtering_and_retention.Compliance._get_employee_data_source")
def test_init_when_no_employee_table(mocked_employee_loader):
    mocked_employee_loader.return_value = None
    compliance = Compliance()
    assert isinstance(compliance._session, FuturesSession)
    assert mocked_employee_loader.call_count == 1
    assert compliance.employee is None

@patch("src.app.filtering_and_retention.Compliance._get_employee_data_source")
@patch("src.app.filtering_and_retention.Compliance._load_config")
@patch("src.app.filtering_and_retention.Compliance._get_fincen_column_relations")
@patch("src.app.filtering_and_retention.Compliance._get_unstructured_column_relations")
def test_init_when_employee_table(mocked_get_unstructured, mocked_get_fincen, mocked_load_config, mocked_employee_loader):
    mocked_employee_loader.return_value = "test_employee"
    compliance = Compliance()
    assert isinstance(compliance._session, FuturesSession)
    assert mocked_employee_loader.call_count == 1
    assert compliance.employee == "test_employee"
    assert compliance._load_config.call_count == 2
    assert isinstance(compliance.value_matching_config, ValueMatchingConfiguration)
    assert isinstance(compliance.row_mapping_config, RowMappingConfiguration)