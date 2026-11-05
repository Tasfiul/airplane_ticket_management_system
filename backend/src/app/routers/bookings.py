from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.booking import Booking
from app.schemas.booking import BookingCreate, BookingResponse
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/bookings", tags=["Bookings"])


@router.post(
    "",
    response_model=BookingResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a booking and purchase tickets (via Redis Stream)",
)
async def create_booking(payload: BookingCreate) -> BookingResponse:
    """Creates a booking and reserves seats.

    All database writes and row-level locking are performed through the Redis Stream worker,
    ensuring zero direct database writes over HTTP.
    """
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_BOOKING,
        entity="booking",
        payload=payload.model_dump(mode="json"),
    )
    return BookingResponse.model_validate(result)


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
