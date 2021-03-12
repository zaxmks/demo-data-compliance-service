from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.core.db.config import DatabaseEnum, get_db_config
from src.core.db.database_config import DatabaseConfig
from src.core.env.env import ApplicationEnv


class SQLEngineFactory:

    def __init__(self):
        self._engines = {}
        self._secrets = {
            DatabaseEnum.MAIN_INGESTION_DB.value: ApplicationEnv.MAIN_DB_SECRET_ID(),
            DatabaseEnum.PDF_INGESTION_DB.value: ApplicationEnv.PDF_DB_SECRET_ID(),
        }

    def create_all_engines(self):
        db_enum_list = [e.value for e in DatabaseEnum]
        for name in db_enum_list:
            cfg: DatabaseConfig = get_db_config(self._secrets[name])
            engine: Engine = create_engine(cfg.make_url(), pool_recycle=3600)
            self._engines[name] = engine

    def get_engine(self, db_name: DatabaseEnum):
        return self._engines[db_name.value]
