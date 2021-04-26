from kfai_sql_chemistry.aws.aws_db_config import AwsDbConfig
from kfai_sql_chemistry.db.main import register_databases
from kfai_sql_chemistry.db.session import AppSession

database_map = {
    "main": AwsDbConfig().detect_db_config("main"),
    "pdf": AwsDbConfig().detect_db_config("pdf"),
    "itact": AwsDbConfig().detect_db_config("itact"),
}


def db_init():
    register_databases(database_map)


def MainDbSession():
    return AppSession("main")


def PdfDbSession():
    return AppSession("pdf")


def ItactDbSession():
    return AppSession("itact")
