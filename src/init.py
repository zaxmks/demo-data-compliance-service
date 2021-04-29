from src.core.db.db_init import db_init
from src.core.env.load import load_env
import logging


def app_init():
    load_env()
    db_init()
