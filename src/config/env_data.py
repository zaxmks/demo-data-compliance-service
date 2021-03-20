import os
from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class EnvData:
    BASE_BUCKET_ID: str = ""
    AWS_APP_SECRET_REGION_ID: str = ""
    # local only
    APP_DATABASE_HOST: str = ""
    APP_DATABASE_PORT: int = None
    APP_DATABASE_NAME: str = ""
    APP_DATABASE_USERNAME: str = ""
    APP_DATABASE_PASSWORD: str = ""
    AWS_DB_SECRET_ID: str = ""

    @classmethod
    def create_env_data(cls):
        return EnvData.from_dict(os.environ)
