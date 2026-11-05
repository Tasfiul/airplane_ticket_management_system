from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.flight import Flight
from app.models.flight_seat import FlightSeat
from app.schemas.flight import (
    FlightCreate,
    FlightResponse,
    FlightSeatResponse,
    FlightStatusUpdate,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.seat_inventory import seat_inventory
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/flights", tags=["Flights"])


@router.post(
    "",
    response_model=FlightResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Schedule a flight and initialize its seats (via Redis Stream)",
)
async def create_flight(payload: FlightCreate) -> FlightResponse:
    """Schedule a new flight.

    Database writes are piped entirely through Redis Stream.
    """
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_FLIGHT,
        entity="flight",
        payload=payload.model_dump(mode="json"),
    )
    return FlightResponse.model_validate(result)


@router.get(
    "/{flight_id}",
    response_model=FlightResponse,
    summary="Get flight details (Cache-Aside)",
)
async def get_flight(
    flight_id: int, db: AsyncSession = Depends(get_db)
) -> FlightResponse:
    """Fetch flight info using cache-aside pattern."""
    cache_key = f"cache:flight:{flight_id}"

    async def fetch_from_db():
        result = await db.execute(select(Flight).where(Flight.flight_id == flight_id))
        flight = result.scalar_one_or_none()
        if not flight:
            return None
        return FlightResponse.model_validate(flight).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=1800,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Flight {flight_id} not found"},
        )

    return FlightResponse.model_validate(cached_data)


@router.get(
    "/{flight_id}/seats",
    response_model=list[FlightSeatResponse],
    summary="Get flight seat availability (Redis Hash Cache-Aside)",
)
async def get_flight_seats(
    flight_id: int, db: AsyncSession = Depends(get_db)
) -> list[FlightSeatResponse]:
    """Fetch seat map for flight directly from Redis Hash in sub-millisecond."""
    seats_data = await seat_inventory.get_or_load_seat_map(flight_id, db, ttl=1800)
    return [FlightSeatResponse.model_validate(s) for s in seats_data]


@router.patch(
    "/{flight_id}/status",
    summary="Update flight status (via Redis Stream)",
)
async def update_flight_status(
    flight_id: int, payload: FlightStatusUpdate
) -> dict:
    """Update flight status strictly via Redis Stream write pipeline."""
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.UPDATE_FLIGHT_STATUS,
        entity="flight",
        payload={"flight_id": flight_id, "status": payload.status},
    )
    return result
