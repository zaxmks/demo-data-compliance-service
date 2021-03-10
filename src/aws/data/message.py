from dataclasses import dataclass
from uuid import UUID

from dataclasses_json import LetterCase, dataclass_json

from src.aws.data.file_message import FileMessage


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class AwsMessage:
    message_id: UUID
    receipt_handle: str
    MD5OfBody: str
    body: FileMessage
