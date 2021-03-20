from typing import List

from src.mapping.entities.entity_relation import EntityRelation
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.values.value_matching_model_factory import ValueMatchingModelFactory
from src.mapping.values.value_matching_target import ValueMatchingTarget


class EntityRelationBuilder:
    def __init__(self, target_source, target_column: str):
        """Instantiate new entity relation builder."""
        self.target_source = target_source
        self.target_column = target_column

    def _build_relations_from_matches(
        self, source_entity: str, matches: list, confidence_threshold: float
    ) -> List[EntityRelation]:
        """Build relation objects from a list of matches."""
        relations = []
        for match in matches:
            match_dict = match.to_dict()
            if match_dict["confidence"] >= confidence_threshold:
                relations.append(
                    EntityRelation(
                        self.target_source,
                        source_entity,
                        self.target_column,
                        match_dict["target_index"],
                        match_dict["target_text"],
                        match_dict["confidence"],
                    )
                )
        return relations

    def get_relations(
        self, entities: list, mapping_configuration: ValueMatchingConfiguration
    ) -> List[EntityRelation]:
        """Get relations based on a mapping configuration object's parameters."""
        matching_target = ValueMatchingTarget(
            self.target_source.get_data()[self.target_column], mapping_configuration
        )
        matching_model = ValueMatchingModelFactory.get_model_from_config(
            mapping_configuration
        )
        all_relations = []
        for entity in entities:
            matches = matching_model.predict(
                entity.get_text(),
                matching_target,
                confidence_threshold=mapping_configuration.get_confidence_threshold(),
            )
            for relation in self._build_relations_from_matches(
                entity, matches, mapping_configuration.get_confidence_threshold(),
            ):
                all_relations.append(relation)

        return all_relations
