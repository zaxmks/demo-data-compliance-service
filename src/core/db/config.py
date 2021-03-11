from enum import Enum

from src.core.aws.clients import get_kf_secrets_manager
from src.core.aws.secrets import SecretValue
from src.core.db.database_config import DatabaseConfig
from src.core.env.env import ApplicationEnv


class DatabaseEnum(Enum):
    PDF_INGESTION_DB = "PDF_INGESTION_DB"
    MAIN_INGESTION_DB = "MAIN_INGESTION_DB"


def get_db_config(aws_secret_id: str) -> DatabaseConfig:
    if ApplicationEnv.IS_DEPLOYED():
        return _get_deployed_config_for_db(aws_secret_id)
    else:
        return _get_local_config()


def _get_deployed_config_for_db(aws_secret_id: str) -> DatabaseConfig:
    secrets = get_kf_secrets_manager()
    raw_secret_value = secrets.get_secret_value(SecretId=aws_secret_id)
    sv = SecretValue(**raw_secret_value)
    return DatabaseConfig.from_json(sv.SecretString)


def _get_local_config() -> DatabaseConfig:
    return DatabaseConfig.from_local_env()
