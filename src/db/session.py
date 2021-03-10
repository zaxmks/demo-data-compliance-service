import logging
import time
from contextlib import contextmanager

import typing

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.db.config import get_db_config
from src.db.database_config import DatabaseConfig

session_obj = sessionmaker()

logger = logging.getLogger(__name__)

cfg: DatabaseConfig = get_db_config()
engine: Engine = create_engine(cfg.make_url(), pool_recycle=3600)


class AppSession:
    def __init__(self):
        self._engine = engine
        self._session_instance = self._create_session()
        # self._session_instance.execute(f"USE postgres")

    def _create_session(self) -> Session:
        # create a configured "Session" class
        session_obj.configure(bind=self._engine)

        # create a Session
        return session_obj()

    @property
    def instance(self) -> Session:
        return self._session_instance


@contextmanager
def DBContext() -> typing.ContextManager[Session]:
    session = AppSession()
    try:
        yield session.instance
        session.instance.commit()
    except Exception as err:
        logger.error(f"Failed to perform query: {err}")
        session.instance.rollback()
        raise err
    finally:
        session.instance.close()


class DbQuery:
    def __init__(self):
        self._total_retries = 10

    def execute(self, query):
        return self._do_query(query, self._total_retries)

    def _do_query(self, query, retries):
        if retries < 0:
            raise

        connection = engine.connect()  # replace your connection
        try:
            return connection.execute(query)
        except Exception as err:  # may need more exceptions here (or trap all)
            connection.close()
            logger.error("Failed to perform query, will do retry")
            logger.error(err)
            # wait before retry
            time.sleep(5)
            self._do_query(query, retries - 1)
        finally:
            connection.close()
