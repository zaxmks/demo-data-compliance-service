import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv


class Environment(Enum):
    local = "local"
    prod = "prod"


def current_env() -> str:
    env = os.getenv("PYTHON_ENV", Environment.local.value)
    return env


def is_local_env():
    return Environment.local.value in current_env()


def is_deployed_env():
    return Environment.prod.value in current_env()


def load_env():
    PYTHON_ENV = current_env()
    # ensuring that we read the .env.XXXX files in the same directory.
    env_dir = os.path.dirname(__file__)
    env_path = Path(env_dir, f".env.{PYTHON_ENV}")
    load_dotenv(verbose=True, dotenv_path=env_path)
