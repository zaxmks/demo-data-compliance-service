from src.core.aws.clients import get_kf_secrets_manager
from src.core.aws.secrets import SecretValue
from src.core.db.database_config import DatabaseConfig
from src.core.env import ApplicationEnv


def get_db_config() -> DatabaseConfig:
    if ApplicationEnv.IS_DEPLOYED():
        return _get_aws_config()
    else:
        return _get_local_config()


def _get_aws_config() -> DatabaseConfig:
    secrets = get_kf_secrets_manager()
    secret_id = ApplicationEnv.PDF_DB_SECRET_ID()
    raw_secret_value = secrets.get_secret_value(SecretId=secret_id)
    sv = SecretValue(**raw_secret_value)
    return DatabaseConfig.from_json(sv.SecretString)


def _get_local_config() -> DatabaseConfig:
    return DatabaseConfig.from_local_env()
