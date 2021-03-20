import json
from typing import Any, List

from spacy import displacy

from src.sources.data_source_base import DataSourceBase
from src.mapping.entities.entity_relation import EntityRelation
from src.mapping.entities.entity_relation_builder import EntityRelationBuilder
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.ner.named_entity import NamedEntity
from src.ner.multi_ner_configuration import MultiNERConfiguration
from src.ner.multi_ner_model_factory import MultiNERModelFactory


class UnstructuredDataSource(DataSourceBase):
    def __init__(self, data: str, name: str):
        """Initialize an unstructured datasource."""
        super().__init__(data=data, structured=False, name=name)
        self.entities = []
        self.entity_relations = []

    def get_displacy_html(self):
        """Create displacy html."""
        input_data = {"text": self.get_data(), "title": None}
        input_data["ents"] = [e.to_displacy_dict() for e in self.entities]
        return displacy.render(
            input_data, style="ent", manual=True, page=False, jupyter=False
        )

    def append_entity(self, entity: NamedEntity):
        """Append a named entity."""
        self.entities.append(entity)

    def get_entities(self, label=None) -> List[NamedEntity]:
        """Return all entities."""
        return [e for e in self.entities if e.get_label() == label or label is None]

    def detect_entities(self, multi_ner_configuration: MultiNERConfiguration):
        """Detect entities based on a predefined configuration."""
        multi_ner_model = MultiNERModelFactory.get_model_from_config(multi_ner_configuration)
        for entity in multi_ner_model.predict(
            self.get_data(), multi_ner_configuration.get_confidence_threshold()
        ):
            self.append_entity(entity)

    def relate_entities_to(
        self,
        target_source,
        target_column: str,
        mapping_configuration: ValueMatchingConfiguration,
        label: str,
    ):
        """Create entity relations to another dataset using a particular model configuration."""
        relation_builder = EntityRelationBuilder(target_source, target_column)
        relations = relation_builder.get_relations(
            self.get_entities(label), mapping_configuration
        )
        for relation in relations:
            self.entity_relations.append(relation)
