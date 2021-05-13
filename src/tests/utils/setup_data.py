import json
import unittest

from kfai_sql_chemistry.db.main import engines
from kfai_sql_chemistry.utils.setup_for_testing import (
    setup_db_for_tests,
    tear_down_db_for_tests,
)

from src.core.db.models.pdf_models import metadata as pdf_metadata
from src.core.db.models.main_models import metadata as main_metadata


def teardown_pdf_db():
    tear_down_db_for_tests(engines.get_engine("pdf"))


def setup_pdf_db():
    setup_db_for_tests(engines.get_engine("pdf"), pdf_metadata)


def teardown_main_db():
    tear_down_db_for_tests(engines.get_engine("main"))


def setup_main_db():
    setup_db_for_tests(engines.get_engine("main"), main_metadata)


class DbTestCase(unittest.TestCase):
    def setUp(self) -> None:
        setup_pdf_db()
        setup_main_db()

    def tearDown(self) -> None:
        teardown_pdf_db()
        teardown_main_db()
