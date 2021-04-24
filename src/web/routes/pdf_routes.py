from fastapi import APIRouter
from pydantic.main import BaseModel

from src.app.filtering_and_retention import Compliance

pdf_router = APIRouter()


class VerifyResponse(BaseModel):
    message: str


@pdf_router.post("/process/{ingestion_event_id}", tags=["process_pdf"])
async def process(ingestion_event_id: str):
    """
    Receive an ingestion_event_id that should be verified by the
    compliance module for making it into the
    main ingestion db
    """
    print("made it to compliance")
    compliance = Compliance()
    result = compliance.filter_and_retain(ingestion_event_id)
    return VerifyResponse(message=result)
