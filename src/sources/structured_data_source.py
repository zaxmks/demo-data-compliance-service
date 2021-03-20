import json
from typing import Any, List

import pandas as pd

from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.columns.column_relation_builder import ColumnRelationBuilder
from src.mapping.columns.pseudocolumn_generator import PseudocolumnGenerator
from src.sources.data_source_base import DataSourceBase
from src.mapping.entities.entity_relation import EntityRelation
from src.mapping.entities.entity_relation_builder import EntityRelationBuilder
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.rows.row_relation import RowRelation
from src.mapping.rows.row_relation_builder import RowRelationBuilder


class StructuredDataSource(DataSourceBase):
    def __init__(self, data: Any, name: str):
        """Initialize a structured data source."""
        super().__init__(data=data, structured=True, name=name)
        self.column_relations = []
        self.row_relations = []

    def append_column(self, data_to_append: list, column_name: str):
        """Append a column to dataframe if structured."""
        if len(data_to_append) != len(self.data):
            raise Exception(
                "Can't append column '%s' of length %d to data source '%s' of length %d"
                % (column_name, len(data_to_append), self.name, len(self.data))
            )
        self.data[column_name] = data_to_append

    def append_column_relation(self, relation: ColumnRelation):
        """Create relation from column in this dataset that corresponds to another."""
        if not isinstance(relation, ColumnRelation):
            raise Exception(
                "Can't append column relation, must be a ColumnRelation object"
            )
        self.column_relations.append(relation)

    def append_row_relation(self, relation: RowRelation):
        """Create relation from row in this (structured) dataset that corresponds to a target row."""
        if not isinstance(relation, RowRelation):
            raise Exception(
                "Can't append row relation, must be a ColumnRelation object"
            )
        self.row_relations.append(relation)

    def create_column_relation(
        self, source_column_name: str, target_column_name: str, target_data_source: Any
    ):
        """Create a column relation manually."""
        self.column_relations.append(
            ColumnRelation(
                target_data_source=target_data_source,
                source_column_name=source_column_name,
                target_column_name=target_column_name,
                confidence=1.0,
            )
        )

    def get_column_relations(self):
        """"Return column relations."""
        return self.column_relations

    def relate_columns_to(
        self, target_source, mapping_configuration: ValueMatchingConfiguration
    ):
        """Create column relations to another dataset using a particular model configuration."""
        relation_builder = ColumnRelationBuilder(self, target_source)
        relations = relation_builder.get_relations(mapping_configuration)
        for relation in relations:
            self.column_relations.append(relation)

    def map_rows_to(
        self,
        target_source,
        value_matching_configuration: ValueMatchingConfiguration,
        row_mapping_configuration: RowMappingConfiguration,
    ):
        """Map rows given required configurations."""
        relation_builder = RowRelationBuilder(self, target_source)
        relations: List[RowRelation] = relation_builder.get_relations(
            value_matching_configuration, row_mapping_configuration
        )
        for relation in relations:
            self.row_relations.append(relation)

    def describe_row_relation_for_index(self, index):
        """Create a JSON string representation of a linked row given an index."""
        results = {}
        for relation in self.get_column_relations():
            source_column = relation.source_column_name
            value = str(self.get_data()[source_column].values[index])
            results[source_column] = value
        return json.dumps(results)

    def get_column(self, column_name):
        """Return values for a particular column name."""
        return self.get_data()[column_name].values

    def get_column_series(self, column_name: str) -> pd.Series:
        """Return values for a particular column name."""
        return self.get_data()[column_name]
