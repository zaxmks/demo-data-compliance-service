import os
from enum import Enum, unique

from dotenv import load_dotenv


@unique
class Environments(Enum):
    LOCAL = "LOCAL"
    PROD = "PROD"


possible_environments = {
    Environments.LOCAL.name: [".local.env", ".local.env.local"],
    Environments.PROD.name: [".prod.env", ".prod.env.local"],
}


def load_env():
    env = os.getenv("ENV", "LOCAL")
    file_path = os.path.abspath(os.path.dirname(__file__))
    for curr_env in possible_environments[env]:
        load_dotenv(dotenv_path=f"{file_path}/{curr_env}", verbose=True, override=True)
