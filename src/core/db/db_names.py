from enum import Enum


# Put here to avoid circular import
class DatabaseEnum(Enum):
    PDF_INGESTION_DB = "PDF_INGESTION_DB"
    MAIN_INGESTION_DB = "MAIN_INGESTION_DB"
    ITACT_INGESTION_DB = "ITACT_INGESTION_DB"
