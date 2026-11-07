from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.booking import Booking
from app.models.customer import Customer
from app.schemas.booking import (
    BookingConfirmRequest,
    BookingCreate,
    BookingInitiateResponse,
    BookingResponse,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/bookings", tags=["Bookings"])


@router.post(
    "",
    summary="Create a booking and purchase tickets (via Redis Stream)",
)
async def create_booking(
    payload: BookingCreate,
    response: Response,
    async_ingest: bool = Query(
        False,
        description="Whether to ingest asynchronously and return 202 Accepted",
    ),
) -> BookingResponse | BookingInitiateResponse:
    """Creates a booking and reserves seats.

    When async_ingest=True, ingests into Redis Stream and returns 202 Accepted immediately.
    When async_ingest=False (default), synchronously waits for DB commit and returns 201 Created.
    """
    if async_ingest:
        event_id = await write_stream_service.dispatch_async(
            action=WriteAction.CREATE_BOOKING,
            entity="booking",
            payload=payload.model_dump(mode="json"),
        )
        response.status_code = status.HTTP_202_ACCEPTED
        return BookingInitiateResponse(
            event_id=event_id,
            status="PENDING",
            message="Booking initiated and queued for asynchronous ingestion",
        )

    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_BOOKING,
        entity="booking",
        payload=payload.model_dump(mode="json"),
    )
    response.status_code = status.HTTP_201_CREATED
    return BookingResponse.model_validate(result)


@router.get(
    "/lookup",
    response_model=BookingResponse,
    summary="Lookup booking by PNR code and passenger last name (Cache-Aside)",
)
async def lookup_booking(
    booking_code: str = Query(..., min_length=4, max_length=16, description="Booking confirmation code (e.g. BK-12345678)"),
    last_name: str = Query(..., min_length=1, max_length=60, description="Customer last name"),
    db: AsyncSession = Depends(get_db),
) -> BookingResponse:
    """Lookup booking by PNR code and last name with Redis cache-aside."""
    norm_code = booking_code.strip().upper()
    norm_last_name = last_name.strip()
    cache_key = f"cache:booking:lookup:{norm_code}:{norm_last_name.lower()}"

    async def fetch_from_db():
        stmt = (
            select(Booking)
            .join(Customer, Booking.customer_id == Customer.customer_id)
            .where(
                Booking.booking_code == norm_code,
                func.lower(Customer.last_name) == norm_last_name.lower(),
            )
            .options(selectinload(Booking.tickets))
        )
        res = await db.execute(stmt)
        booking = res.scalar_one_or_none()
        if not booking:
            return None
        return BookingResponse.model_validate(booking).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=1800,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Not Found",
                "message": f"No booking found matching code {booking_code} and last name {last_name}",
            },
        )

    return BookingResponse.model_validate(cached_data)


@router.get(
    "/{booking_id}",
    response_model=BookingResponse,
    summary="Get booking details (Cache-Aside)",
)
async def get_booking(
    booking_id: int, db: AsyncSession = Depends(get_db)
) -> BookingResponse:
    """Fetch booking details using cache-aside pattern."""
    cache_key = f"cache:booking:{booking_id}"

    async def fetch_from_db():
        stmt = (
            select(Booking)
            .where(Booking.booking_id == booking_id)
            .options(selectinload(Booking.tickets))
        )
        res = await db.execute(stmt)
        booking = res.scalar_one_or_none()
        if not booking:
            return None
        return BookingResponse.model_validate(booking).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=1800,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Booking {booking_id} not found"},
        )

    return BookingResponse.model_validate(cached_data)


@router.post(
    "/{booking_id}/confirm",
    response_model=BookingResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Confirm and purchase a pending booking (via Redis Stream)",
)
async def confirm_booking(
    booking_id: int,
    payload: BookingConfirmRequest | None = None,
) -> BookingResponse:
    """Confirms a pending booking and finalizes payment.

    Synchronously waits for the PostgreSQL transaction to commit via PgBouncer before
    returning HTTP 201 Created, preventing phantom tickets.
    """
    body_data = payload.model_dump(mode="json") if payload else {}
    body_data["booking_id"] = booking_id

    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CONFIRM_BOOKING,
        entity="booking",
        payload=body_data,
    )
    return BookingResponse.model_validate(result)


@router.post(
    "/{booking_id}/cancel",
    summary="Cancel booking and release seats (via Redis Stream)",
)
async def cancel_booking(booking_id: int) -> dict:
    """Cancels booking and releases seats back to AVAILABLE via Redis Stream."""
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CANCEL_BOOKING,
        entity="booking",
        payload={"booking_id": booking_id},
    )
    return result
