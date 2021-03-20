import os
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import LetterCase, dataclass_json
from sqlalchemy.engine.url import URL
from src.core.db.db_names import DatabaseEnum


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class DatabaseConfig:
    username: str
    password: str
    engine: str
    host: str
    port: int
    db_cluster_identifier: Optional[str] = ""
    db_instance_identifier: Optional[str] = ""
    db_name: Optional[str] = ""

    @staticmethod
    def from_local_env(name: DatabaseEnum):
        if name == DatabaseEnum.PDF_INGESTION_DB.value:
            return DatabaseConfig(
                username=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                engine=os.getenv("DB_ENGINE"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT")),
                db_name=os.getenv("DB_NAME"),
                db_cluster_identifier=os.getenv("DB_CLUSTER_IDENTIFIER"),
            )
        elif name == DatabaseEnum.MAIN_INGESTION_DB.value:
            return DatabaseConfig(
                username=os.getenv("MAIN_DB_USERNAME"),
                password=os.getenv("MAIN_DB_PASSWORD"),
                engine=os.getenv("MAIN_DB_ENGINE"),
                host=os.getenv("MAIN_DB_HOST"),
                port=int(os.getenv("MAIN_DB_PORT")),
                db_name=os.getenv("MAIN_DB_NAME"),
                db_cluster_identifier=os.getenv("MAIN_DB_CLUSTER_IDENTIFIER"),
            )
        elif name == DatabaseEnum.ITACT_INGESTION_DB.value:
            return DatabaseConfig(
                username=os.getenv("ITACT_DB_USERNAME"),
                password=os.getenv("ITACT_DB_PASSWORD"),
                engine=os.getenv("ITACT_DB_ENGINE"),
                host=os.getenv("ITACT_DB_HOST"),
                port=int(os.getenv("ITACT_DB_PORT")),
                db_name=os.getenv("ITACT_DB_NAME"),
                db_cluster_identifier=os.getenv("ITACT_DB_CLUSTER_IDENTIFIER"),
            )
        else:
            raise RuntimeError(f'No database: {name}')


    @staticmethod
    def from_local_env_main_db():
        return DatabaseConfig(
            username=os.getenv("MAIN_DB_USERNAME"),
            password=os.getenv("MAIN_DB_PASSWORD"),
            engine=os.getenv("MAIN_DB_ENGINE"),
            host=os.getenv("MAIN_DB_HOST"),
            port=int(os.getenv("MAIN_DB_PORT")),
            db_name=os.getenv("MAIN_DB_NAME"),
            db_cluster_identifier=os.getenv("MAIN_DB_CLUSTER_IDENTIFIER"),
        )

    def make_url(self):
        return URL(
            self.engine,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database="postgres",
        )
