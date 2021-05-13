from src.app.filtering.affiliate_filter import AffiliateFilter


class AffiliateFilterFincen(AffiliateFilter):

    def __init__(self, column_relations, affiliates):
        self.column_relations = column_relations
        self.affiliates = affiliates

    def filter_affiliates(self, ingestion_event_id, config):
        df = self._get_pdf_document(ingestion_event_id)
        self.f_vals = df.to_dict(orient="records")[
            0
        ]  # assume one doc per ingestion_event
        self.results_df = self._build_results_df(df, self.column_relations, config)
        self.num_records = self.results_df.shape[0]