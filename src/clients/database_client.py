import os
from typing import List

import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import testing.postgresql

from src.config.database_config import DatabaseConfig


class DatabaseClient:
    def __init__(
        self,
        database=None,
        host=None,
        username=None,
        password=None,
        port=5432,
        temp=False,
    ):
        """Initialize new database client."""
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.temp = temp
        self.connection = None
        self.engine = None

    @classmethod
    def create_by_db_config(cls, db_config: DatabaseConfig):
        return cls(
            username=db_config.username,
            database=db_config.db,
            host=db_config.host,
            password=db_config.password,
            port=db_config.port,
        )

    @classmethod
    def create_by_env(cls):
        db_config = DatabaseConfig.create_config_by_env()
        return cls(
            username=db_config.username,
            database=db_config.db,
            host=db_config.host,
            password=db_config.password,
            port=db_config.port,
        )

    def connect(self):
        """Connect to the database given the specified connection parameters."""
        if self.engine is None:
            if self.temp:
                self.psql = testing.postgresql.Postgresql()
                self.engine = create_engine(self.psql.url())
                self.connection = psycopg2.connect(**self.psql.dsn())
            else:
                try:
                    self.engine = create_engine(
                        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                            self.username,
                            self.password,
                            self.host,
                            self.port,
                            self.database,
                            pool_size=10,
                            max_overflow=0,
                        )
                    )
                    self.connection = self.engine.raw_connection()
                except sqlalchemy.exc.OperationalError:
                    raise Exception(
                        "Could not connect to database given the current environment."
                    )

    def execute(self, query) -> List[tuple]:
        """Execute a single query, fetching the results if they exist."""
        if self.connection is None:
            self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            self.connection.rollback()
            raise Exception(e)
        self.connection.commit()
        try:
            results = cursor.fetchall()
        except psycopg2.ProgrammingError:
            results = None
        cursor.close()
        return results

    def get_table_names(self) -> List[str]:
        """Get all table names (data sources) in the database."""
        results = self.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE"
            " table_schema != 'information_schema' AND table_schema != 'pg_catalog'"
        )
        return [".".join(r) for r in results]

    def close(self):
        """Close an existing connection."""
        if self.connection is not None:
            try:
                self.connection.close()
                self.engine.dispose()
            except:
                pass
        self.engine = None
        self.connection = None

    def read_sql(self, query, parse_dates=None) -> pd.DataFrame:
        """Read SQL query into a pandas dataframe from the remote database, while parsing specified columns as dates.."""
        if self.engine is None:
            self.connect()
        return pd.read_sql(query, self.engine, parse_dates=parse_dates)
