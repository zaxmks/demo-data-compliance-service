import json
from typing import Any, List

from spacy import displacy

from src.clients.database_client import DatabaseClient
from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.columns.column_relation_builder import ColumnRelationBuilder
from src.mapping.columns.pseudocolumn_generator import PseudocolumnGenerator
from src.sources.data_loader import DataLoader
from src.mapping.entities.entity_relation import EntityRelation
from src.mapping.entities.entity_relation_builder import EntityRelationBuilder
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.rows.row_relation import RowRelation
from src.mapping.rows.row_relation_builder import RowRelationBuilder
from src.ner.named_entity import NamedEntity
from src.ner.ner_configuration import NERConfiguration
from src.ner.ner_model_factory import NERModelFactory


class DataSourceBase:
    def __init__(self, data: Any, structured: bool, name: str):
        """Initialize a generic data source."""
        self.data = data
        self.structured = structured
        self.name = name

    def __str__(self) -> str:
        """Get name of this object."""
        return self.name

    def __repr__(self) -> str:
        """Get string representation."""
        return self.name

    def __len__(self) -> int:
        """Get length of data source (structured or unstructured)."""
        return len(self.get_data())

    def is_structured(self) -> bool:
        """Detect if the data source is structured."""
        return self.structured

    def get_data(self):
        """Return data from the source. If structured, a pandas dataframe is returned. If unstructured,
        the full text is returned as a string.
        """
        return self.data
