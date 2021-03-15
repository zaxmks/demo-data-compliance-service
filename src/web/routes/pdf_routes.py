from enum import Enum
from typing import List

from fastapi import APIRouter
from pydantic.main import BaseModel


pdf_router = APIRouter()


class DatabaseEnum(Enum):
    PDF = "PDF"
    ITACT = "ITACT"


class VerifyDataInput(BaseModel):
    ids: List[str]
    database: DatabaseEnum


class VerifyResponse(BaseModel):
    message: str


@pdf_router.post("/verify_data", tags=["verify"])
async def bulk_verify(body: VerifyDataInput):
    """
    Receive a set of ids that should be verified by the compliance module for making it into the
    main ingestion db
    """
    print(body)
    return VerifyResponse(message="OK")
