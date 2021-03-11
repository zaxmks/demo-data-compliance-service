import logging
import logging.handlers
from logging.config import dictConfig

logger = logging.getLogger(__name__)

DEFAULT_LOGGING = {"version": 1, "disable_existing_loggers": False}


def configure_logging():
    """
    Initialize logging defaults for Project.

    :param logfile_path: logfile used to the logfile
    :type logfile_path: string

    This function does:

    - Assign INFO and DEBUG level to logger file handler and console handler

    """
    dictConfig(DEFAULT_LOGGING)

    default_formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)8s] [%(name)s] [%(funcName)s():%(lineno)s] [PID:%(process)d "
        "TID:%(thread)d] -> "
        "%(message)s",
        "%d/%m/%Y %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(default_formatter)
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(console_handler)

    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    # logging.root.setLevel(logging.DEBUG)
