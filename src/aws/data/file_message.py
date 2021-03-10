from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class FileMessage:
    s3_bucket: str
    s3_key: str
