from fastapi import APIRouter
from starlette.responses import Response

from src.app.filtering_and_retention import Compliance

pdf_router = APIRouter()


@pdf_router.post("/process/{ingestion_event_id}", tags=["process_pdf"])
async def process(ingestion_event_id: str):
    """
    Receive an ingestion_event_id that should be verified by the
    compliance module for making it into the
    main ingestion db
    """
    compliance = Compliance()
    result = compliance.filter_and_retain(ingestion_event_id)
    return Response(content=result)
