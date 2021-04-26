import logging
import unittest

from kfai_sql_chemistry.db.main import engines
from kfai_sql_chemistry.utils.setup_for_testing import (
    setup_db_for_tests,
    tear_down_db_for_tests,
)

from src.core.db.db_init import MainDbSession, PdfDbSession, db_init
from src.core.db.models import main_models, pdf_models


class TestDbCase(unittest.TestCase):
    def setUp(self):
        db_init()
        setup_db_for_tests(engines.get_engine("main"), main_models.metadata)
        setup_db_for_tests(engines.get_engine("pdf"), pdf_models.metadata)

    def tearDown(self):
        tear_down_db_for_tests(engines.get_engine("main"))
        tear_down_db_for_tests(engines.get_engine("pdf"))

    def test_db_init(self):

        with MainDbSession() as session:
            a = session.execute("SELECT * FROM compliance_run_event")
            logging.getLogger().info(a.fetchall())

        with PdfDbSession() as session:
            a = session.execute("SELECT * FROM parsing_strategy_type")
            logging.getLogger().info(a.fetchall())
