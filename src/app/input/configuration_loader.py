import json
from typing import List

from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration


class ConfigurationLoader(object):

    def __init__(self, value_matching_config_json, row_mapping_config_json):
        self.value_matching_config_json = value_matching_config_json
        self.row_mapping_config_json = row_mapping_config_json
        self.value_matching_config = ValueMatchingConfiguration(
            **self._load_config(value_matching_config_json)
        )
        self.row_mapping_config = RowMappingConfiguration(**self._load_config(row_mapping_config_json))
        self.fincen_column_relations = self._get_fincen_column_relations()
        self.unstructured_column_relations = (
            self._get_unstructured_column_relations()
        )

    @staticmethod
    def _get_fincen_column_relations() -> List[ColumnRelation]:
        column_relations = [
            ColumnRelation("employee", "tin", "ssn", 1.0),
            ColumnRelation("employee", "dob", "date_of_birth", 1.0),
            ColumnRelation("employee", "first_name", "first_name", 1.0),
            ColumnRelation("employee", "last_name", "last_name", 1.0),
        ]
        return column_relations

    @staticmethod
    def _get_unstructured_column_relations() -> List[ColumnRelation]:
        column_relations = [
            ColumnRelation("employee", "ssn", "ssn", 1.0),
            ColumnRelation("employee", "date_of_birth", "date_of_birth", 1.0),
            ColumnRelation("employee", "first_name", "first_name", 1.0),
            ColumnRelation("employee", "last_name", "last_name", 1.0),
        ]
        return column_relations

    @staticmethod
    def _load_config(path) -> dict:
        """
        Read JSON from a filepath
        """
        with open(path, "r") as F:
            return json.load(F)