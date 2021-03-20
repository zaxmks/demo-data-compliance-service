from dataclasses import dataclass
from dataclasses_json import dataclass_json

from src.config.env_data import EnvData
from src.config.secrets_db_config import _SecretsDBConfig
from src.config.settings import is_local_env


@dataclass_json
@dataclass
class DatabaseConfig:
    """
    This config object services as the public object that consuming DB services will use as
    connection information
    """

    username: str
    password: str
    db: str
    host: str
    port: int

    @classmethod
    def create_config_by_env(cls):
        if is_local_env():
            env_data: EnvData = EnvData.create_env_data()
            return DatabaseConfig(
                username=env_data.APP_DATABASE_USERNAME,
                password=env_data.APP_DATABASE_PASSWORD,
                host=env_data.APP_DATABASE_HOST,
                port=env_data.APP_DATABASE_PORT,
                db=env_data.APP_DATABASE_NAME,
            )

        config = _SecretsDBConfig.create_db_config()
        return DatabaseConfig(
            username=config.username,
            password=config.password,
            host=config.host,
            port=config.port,
            db=config.db,
        )
