from typing import List

from src.mapping.rows.row_mapping_configuration import RowMappingConfiguration
from src.mapping.rows.row_mapping_model_factory import RowMappingModelFactory
from src.mapping.rows.row_relation import RowRelation
from src.mapping.values.value_matching_configuration import ValueMatchingConfiguration
from src.mapping.values.value_matching_model_factory import ValueMatchingModelFactory
from src.mapping.values.value_matching_target import ValueMatchingTarget
from src.mapping.values.value_match import ValueMatch


class RowRelationBuilder:
    def __init__(self, source, target):
        """Create a new instance of a row relation builder."""
        if len(source.get_column_relations()) == 0:
            raise Exception(
                "Must have at least one column relation to build row relations"
            )
        if not source.is_structured() or not target.is_structured():
            raise Exception(
                "Both source and target data sets must be structured to run a row relation operation"
            )
        self.source = source
        self.target = target
        self.value_match_lists = [[] for _ in range(len(self.source))]

    def _get_value_matches(self, mapping_configuration: ValueMatchingConfiguration):
        """Get predicted value matches for every column relation."""
        matching_model = ValueMatchingModelFactory.get_model_from_config(
            mapping_configuration
        )
        for column_relation in self.source.get_column_relations():
            source_column = column_relation.get_source_column_name()
            target_column = column_relation.get_target_column_name()
            matching_target = ValueMatchingTarget(
                self.target.get_column_series(target_column), mapping_configuration
            )
            for i, source_value in enumerate(self.source.get_column(source_column)):
                for match in matching_model.predict(
                    source_value,
                    matching_target,
                    confidence_threshold=mapping_configuration.get_confidence_threshold(),
                ):
                    match.set_source_column(source_column)
                    match.set_target_column(target_column)
                    self.value_match_lists[i].append(match)

    def _rowwise_comparisons(
        self, v_config: ValueMatchingConfiguration, r_config: RowMappingConfiguration
    ) -> List[RowRelation]:
        m_target = ValueMatchingTarget(config=v_config)
        row_model = RowMappingModelFactory.get_model_from_config(r_config)
        val_model = ValueMatchingModelFactory.get_model_from_config(v_config)
        row_thresh = r_config.get_confidence_threshold()
        col_relations = self.source.get_column_relations()
        row_relations = []
        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            "000000000000000000000000000000000000000000000000000000000000000000000"
        )
        logger.info(self.source.get_data().head())
        logger.info(
            "100000000000000000000000000000000000000000000000000000000000000000000"
        )
        logger.info(self.target.get_data().head())
        for s_row in self.source.get_data().iterrows():
            s_i = s_row[0]  # iterrows returns (Index,Series) pair
            logger.info(f"ssss {s_i}")
            for t_row in self.target.get_data().iterrows():
                t_i = t_row[0]
                val_matches: List[ValueMatch] = []
                for column_relation in col_relations:
                    source_column = column_relation.get_source_column_name()
                    target_column = column_relation.get_target_column_name()
                    s_val = s_row[1][source_column]  # iterrows gives (Index,Series)
                    t_val = t_row[1][target_column]
                    logger.info(f"source_val {s_val}, target_val {t_val}")
                    val_confidence = val_model.predict_single(s_val, t_val, m_target)
                    logger.info(val_confidence)
                    val_match = ValueMatch(
                        target_index=t_i,
                        confidence=val_confidence,
                        target_text=t_val,
                        source_column=source_column,
                        target_column=target_column,
                    )
                    val_matches.append(val_match)
                row_confidence, row_match_desc = row_model.predict(
                    col_relations, val_matches, is_return_explanation=True
                )
                if row_confidence > row_thresh:
                    row_relation = RowRelation(
                        target_data_source=self.target,
                        source_index=s_i,
                        target_index=t_i,
                        confidence=row_confidence,
                        match_description=row_match_desc,
                    )
                    logger.info(f"relation: {s_i}, {t_i}")
                    row_relations.append(row_relation)
        return row_relations

    def get_relations(
        self,
        value_matching_configuration: ValueMatchingConfiguration,
        row_mapping_configuration: RowMappingConfiguration,
    ) -> List[RowRelation]:
        """Get relations from a set of configurations."""
        row_relations = self._rowwise_comparisons(
            value_matching_configuration, row_mapping_configuration
        )
        return row_relations
