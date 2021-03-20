import os
from dataclasses import dataclass

from src.core.env.load import Environments


@dataclass
class ApplicationEnv:

    @staticmethod
    def PDF_DB_SECRET_ID():
        return os.getenv("PDF_DB_SECRET_ID", None)

    @staticmethod
    def MAIN_DB_SECRET_ID():
        return os.getenv("MAIN_DB_SECRET_ID", None)

    @staticmethod
    def ITACT_DB_SECRET_ID():
        return os.getenv("ITACT_DB_SECRET_ID", None)


    @staticmethod
    def AWS_PROFILE():
        return os.getenv("AWS_PROFILE", None)

    @staticmethod
    def AWS_DEFAULT_REGION_NAME():
        return os.getenv("AWS_DEFAULT_REGION_NAME", None)

    @staticmethod
    def IS_DEPLOYED():
        return os.getenv("ENV") == Environments.PROD.value

    @staticmethod
    def WORKER_QUEUE_URL():
        return os.getenv("WORKER_QUEUE_URL", None)

    @staticmethod
    def KF_PDF_URL():
        return os.getenv("KF_PDF_URL", None)
