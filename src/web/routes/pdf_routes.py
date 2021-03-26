from enum import Enum
from typing import List

from fastapi import APIRouter
from pydantic.main import BaseModel

from src.app.filtering_and_retention import filter_and_retain


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

if 0:
    @pdf_router.post("/process")
    async def process():
        """
        Receive an ingestion_event_id that should be verified by the
        compliance module for making it into the
        main ingestion db
        """
        ingestion_event_id: str = 'aa'
        result = filter_and_retain(ingestion_event_id)
        return VerifyResponse(message=result)

@pdf_router.post("/demo_db", tags=["verify"])
async def run_demo_db():
    """
    Receive a set of ids that should be verified by the compliance module for making it into the
    main ingestion db
    """
    return VerifyResponse(message="OK")