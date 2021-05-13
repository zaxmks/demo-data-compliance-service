import logging

from src.app.document_type_enum import DocumentTypeEnum
from src.app.filtering.affiliate_filter_fincen import AffiliateFilterFincen
from src.app.filtering.affiliate_filter_unknown import AffiliateFilterUnknown

from src.log_util import log

logger = logging.getLogger(__name__)



class AffiliateFilterFactory(object):

    def build_affiliate_filter(self, doc_type_name, affiliates, config):
        if not doc_type_name:
            message = "Could not find anything for the ingestion event ID."
            log(logger, message, use_delimiter=True, use_disclaimer=True)
            return "ingestion_event_id or document_type not found"
        log(logger, f"Found document of type: {doc_type_name}")
        if doc_type_name == DocumentTypeEnum.FINCEN8300.value:
            column_relations = config.fincen_column_relations
            return AffiliateFilterFincen(column_relations, affiliates)
        elif doc_type_name == DocumentTypeEnum.UNKNOWN.value:
            column_relations = config.unstructured_column_relations
            return AffiliateFilterUnknown(column_relations, affiliates)
        else:
            return "Invalid document type. Request rejected"