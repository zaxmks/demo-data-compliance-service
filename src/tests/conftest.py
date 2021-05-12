import logging
import os

import pytest

from src.core.db.db_init import PdfDbSession, MainDbSession
from src.init import app_init


def pytest_configure():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


def pytest_runtest_setup():
    app_init()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


@pytest.fixture(scope="class")
def testCardDb(r):
    pdf_session = PdfDbSession()
    r.cls.pdf_db = pdf_session

    main_session = MainDbSession()
    r.cls.main_db = main_session