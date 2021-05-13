import logging

from pandas import DataFrame

from src.sources.data_source import DataSource
from src.log_util import log

logger = logging.getLogger(__name__)

class AffiliateFilter(object):

    def __init__(self, column_relations, affiliates):
        self.column_relations = column_relations
        self.affiliates = affiliates

    def gather_matches(self):
        affiliate_ids = []
        for i in range(self.num_records):
            row = self.results_df.iloc[i]
            affiliate_ids.append(row.employee_id)
            message = f"Found match for {row.first_name} {row.last_name} "
            message += f"with confidence {row.confidence} computed by {row.explanation}"
            log(logger, message, use_delimiter=True)
        return affiliate_ids

    def _build_results_df(self, df, column_relations, config):
        ds = DataSource(df)
        ds.column_relations = column_relations
        ds.map_rows_to(self.affiliates, config.value_matching_config, config.row_mapping_config)
        return self._generate_structured_row_matches(ds)

    def _generate_structured_row_matches(self, source: DataSource) -> DataFrame:
        """Generate structured row matches."""
        rows = {
            "first_name": [],  # just for sanity check
            "last_name": [],  # just for sanity check
            "employee_id": [],
            "confidence": [],
            "explanation": [],
        }

        # noinspection PyUnresolvedReferences
        for relation in source.row_relations:
            source_index = relation.source_index
            target_index = relation.target_index
            # noinspection PyUnresolvedReferences
            source_row = source.get_data().iloc[source_index]
            # noinspection PyUnresolvedReferences
            target_row = self.affiliates.get_data().iloc[target_index]
            rows["employee_id"].append(target_row.id)
            rows["first_name"].append(source_row.first_name)
            rows["last_name"].append(source_row.last_name)
            rows["confidence"].append(relation.confidence)
            rows["explanation"].append(relation.match_description)
        return DataFrame(rows)