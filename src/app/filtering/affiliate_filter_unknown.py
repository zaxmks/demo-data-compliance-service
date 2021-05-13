from src.app.filtering.affiliate_filter import AffiliateFilter
from src.app.filtering.unstructured_filter import UnstructuredFilter


class AffiliateFilterUnknown(AffiliateFilter):

    def __init__(self, column_relations, affiliates):
        self.column_relations = column_relations
        self.affiliates = affiliates

    def filter_affiliates(self, ingestion_event_id, config):
        unstructured_filter = UnstructuredFilter()
        people_match_df, self.doc_text = unstructured_filter.filter(ingestion_event_id)
        people_match_df.index = range(people_match_df.shape[0])
        if len(people_match_df) > 0:
            self.f_vals_list = people_match_df.to_dict(orient="records")
            self.results_df = self._build_results_df(people_match_df, self.column_relations, config)
            self.num_records = self.results_df.shape[0]