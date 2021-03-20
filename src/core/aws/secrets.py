from dataclasses import dataclass
from datetime import datetime

from typing import List


@dataclass
class SecretValue:
    ARN: str
    Name: str
    VersionId: str
    SecretString: str
    VersionStages: List[str]
    CreatedDate: datetime
    ResponseMetadata: "ResponseMetadatum"


@dataclass
class ResponseMetadatum:
    RequestId: str
    HttpStatusCode: int
    HTTPHeader: "HttpHeader"
    RetryAttempts: int


@dataclass
class HttpHeader:
    date_: str
    contenttype: str
    contentlength: str
    connection: str
    xamznrequestid: str
