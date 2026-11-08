from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from fastapi.responses import RedirectResponse
import orjson
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.booking import Booking
from app.models.flight import Flight
from app.models.flight_seat import FlightSeat
from app.models.ticket import Ticket
from app.schemas.booking import TicketResponse
from app.services.cache import cache_service
from app.services.minio_storage import minio_service

router = APIRouter(prefix="/tickets", tags=["Tickets"])


@router.get(
    "/{ticket_id}",
    response_model=TicketResponse,
    operation_id="get_ticket",
    responses={
        200: {
            "model": TicketResponse,
            "description": "Ticket details",
            "links": {
                "DownloadTicketPdf": {
                    "operationId": "download_ticket_pdf",
                    "parameters": {"ticket_id": "$response.body#/ticket_id"},
                },
            },
        },
        404: {"description": "Ticket not found"},
    },
    summary="Get ticket details (Cache-Aside)",
)
async def get_ticket(
    ticket_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Ticket ID"),
) -> Response:
    """Fetch ticket details using cache-aside pattern."""
    cache_key = f"cache:ticket:{ticket_id}"

    async def fetch_from_db():
        from app.core.database import async_session_factory
        async with async_session_factory() as session:
            stmt = select(Ticket).where(Ticket.ticket_id == ticket_id)
            res = await session.execute(stmt)
            ticket = res.scalar_one_or_none()
            if not ticket:
                return None
            return TicketResponse.model_validate(ticket).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=1800,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Ticket {ticket_id} not found"},
        )

    return Response(content=orjson.dumps(cached_data), media_type="application/json")


@router.get(
    "/{ticket_id}/pdf",
    operation_id="download_ticket_pdf",
    responses={
        307: {"description": "Temporary Redirect to presigned S3 download URL"},
        404: {"description": "Ticket not found"},
    },
    summary="Download ticket boarding pass PDF (Presigned S3 URL Redirect)",
)
async def download_ticket_pdf(
    ticket_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Ticket ID"),
    redirect: bool = Query(
        True,
        description="Whether to return a 307 Temporary Redirect to the presigned S3 download URL",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Generates or retrieves ticket boarding pass PDF from MinIO vault and returns a presigned URL.

    Per specification, returns HTTP 307 Temporary Redirect to a 15-minute presigned download URL.
    """
    stmt = (
        select(Ticket)
        .where(Ticket.ticket_id == ticket_id)
        .options(
            selectinload(Ticket.booking),
            selectinload(Ticket.flight_seat).selectinload(FlightSeat.flight),
        )
    )
    res = await db.execute(stmt)
    ticket = res.scalar_one_or_none()
    if not ticket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Ticket {ticket_id} not found"},
        )

    booking_code = ticket.booking.booking_code if ticket.booking else "TICKETS"
    object_key = f"tickets/{booking_code}/{ticket_id}.pdf"

    # If not yet generated in MinIO, render on-demand in memory and upload
    if not minio_service.object_exists(object_key):
        flight_num = "N/A"
        dep_airport = "N/A"
        arr_airport = "N/A"
        dep_time = None
        if ticket.flight_seat and ticket.flight_seat.flight:
            fl = ticket.flight_seat.flight
            flight_num = fl.flight_number
            dep_airport = fl.departure_airport
            arr_airport = fl.arrival_airport
            dep_time = fl.departure_time

        ticket_payload = {
            "booking_code": booking_code,
            "ticket_id": ticket.ticket_id,
            "passenger_name": ticket.passenger_name,
            "price": str(ticket.price),
            "status": ticket.status,
            "flight_seat_id": ticket.flight_seat_id,
            "flight_number": flight_num,
            "departure_airport": dep_airport,
            "arrival_airport": arr_airport,
            "departure_time": dep_time.isoformat() if dep_time else None,
        }
        pdf_bytes = minio_service.generate_boarding_pass_pdf(ticket_payload)
        minio_service.upload_ticket_pdf(booking_code, ticket_id, pdf_bytes)

    presigned_url = minio_service.get_presigned_download_url(
        object_key=object_key,
        expires_seconds=900,
    )

    if redirect:
        return RedirectResponse(
            url=presigned_url,
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    return {
        "ticket_id": ticket_id,
        "booking_code": booking_code,
        "download_url": presigned_url,
        "expires_in_seconds": 900,
    }
