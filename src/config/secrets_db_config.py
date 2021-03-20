import logging
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from src.clients.secrets import get_secret
from src.config.env_data import EnvData


logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class _SecretsDBConfig:
    """
    This is a helper class used by the Database Config class. It shouldn't be used outside of the
    Database Config Module
    """

    username: str
    password: str
    engine: str
    host: str
    port: str
    dbInstanceIdentifier: str
    db: str

    @classmethod
    def create_db_config(cls):
        env = EnvData.create_env_data()
        db_config = get_secret(env.AWS_DB_SECRET_ID, env.AWS_APP_SECRET_REGION_ID)
        return cls.from_json(db_config)
