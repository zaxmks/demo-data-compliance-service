from typing import List

from src.mapping.columns.column_relation import ColumnRelation
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.values.value_matching_model_factory import ValueMatchingModelFactory
from src.mapping.values.value_matching_target import ValueMatchingTarget


class ColumnRelationBuilder:
    def __init__(self, source, target):
        """Create a new instance of a column relation builder."""
        if not source.is_structured() or not target.is_structured():
            raise TypeError("To match columns, both sources should be structured.")
        self.source = source
        self.target = target

    def _build_relations_from_matches(
        self, source_column_name: str, matches: list, confidence_threshold: float
    ) -> List[ColumnRelation]:
        """Analyze matches and convert to a match if appropriate."""
        relations = []
        for match in matches:
            match_dict = match.to_dict()
            if match_dict["confidence"] >= confidence_threshold:
                relations.append(
                    ColumnRelation(
                        self.target,
                        source_column_name,
                        match_dict["target_text"],
                        match_dict["confidence"],
                    )
                )
        return relations

    def get_relations(
        self, mapping_configuration: ValueMatchingConfiguration
    ) -> List[ColumnRelation]:
        """Get the column relations based on a mapping configuration."""
        map_by = mapping_configuration.get_map_by_type()
        if map_by == "name":
            return self._get_relations_by_name(mapping_configuration)
        elif map_by == "config":
            raise NotImplementedError
        else:
            raise NotImplementedError(
                "Currently only supports map by 'name', not map by '%s' (check mapping config)"
                % map_by
            )

    def _get_relations_by_name(
        self, mapping_configuration: ValueMatchingConfiguration
    ) -> List[ColumnRelation]:
        """Create column relations if names match above a certain threshold."""
        matching_target = ValueMatchingTarget(
            self.target.get_data().columns, mapping_configuration
        )
        matching_model = ValueMatchingModelFactory.get_model_from_config(
            mapping_configuration
        )
        source_column_names = self.source.get_data().columns
        all_relations = []
        for source_column_name in source_column_names:
            matches = matching_model.predict(
                source_column_name,
                matching_target,
                confidence_threshold=mapping_configuration.get_confidence_threshold(),
            )
            for relation in self._build_relations_from_matches(
                source_column_name,
                matches,
                mapping_configuration.get_confidence_threshold(),
            ):
                all_relations.append(relation)

        return all_relations
