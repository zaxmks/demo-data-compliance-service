from src.core.aws.clients import get_kf_secrets_manager
from src.core.aws.secrets import SecretValue
from src.core.db.database_config import DatabaseConfig
from src.core.db.db_names import DatabaseEnum
from src.core.env.env import ApplicationEnv


def get_db_config(aws_secret_id: str, name: str) -> DatabaseConfig:
    if ApplicationEnv.IS_DEPLOYED():
        return _get_deployed_config_for_db(aws_secret_id)
    else:
        return _get_local_config(name)


def _get_deployed_config_for_db(aws_secret_id: str) -> DatabaseConfig:
    secrets = get_kf_secrets_manager()
    raw_secret_value = secrets.get_secret_value(SecretId=aws_secret_id)
    sv = SecretValue(**raw_secret_value)
    return DatabaseConfig.from_json(sv.SecretString)


def _get_local_config(name: str) -> DatabaseConfig:
    return DatabaseConfig.from_local_env(name)
