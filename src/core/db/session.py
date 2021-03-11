import logging
import time
from contextlib import contextmanager

import typing

from sqlalchemy.orm import Session, sessionmaker

from src.core.db.config import DatabaseEnum
from src.core.db import DBConnections

session_obj = sessionmaker()

logger = logging.getLogger(__name__)

connections = DBConnections()
connections.create_all_connections()


class AppSession:
    def __init__(self, db_enum_type: DatabaseEnum):
        self._engine = connections.get_engine(db_enum_type)
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
def DBContext(enum: DatabaseEnum) -> typing.ContextManager[Session]:
    """
    Use the DatabaseEnum to select the database you want to use
    :param enum:
    :return:
    """
    session = AppSession(enum)
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
    def __init__(self, db_enum: DatabaseEnum):
        self._total_retries = 10
        self._db_enum = db_enum

    def execute(self, query):
        return self._do_query(query, self._total_retries)

    def _do_query(self, query, retries):
        if retries < 0:
            raise

        engine = connections.get_engine(self._db_enum)
        connection = engine.connect() # replace your connection
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


with DBContext(DatabaseEnum.PDF_INGESTION_DB) as c:
    test = c.execute("SELECT * from fincen8300_rev4")
    print(test)
