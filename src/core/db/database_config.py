import os
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import LetterCase, dataclass_json
from sqlalchemy.engine.url import URL


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
    def from_local_env():
        return DatabaseConfig(
            username=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            engine=os.getenv("DB_ENGINE"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            db_name=os.getenv("DB_NAME"),
            db_cluster_identifier=os.getenv("DB_CLUSTER_IDENTIFIER"),
        )

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
