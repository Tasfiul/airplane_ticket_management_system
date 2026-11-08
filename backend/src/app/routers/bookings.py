from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
import orjson
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
from app.services.seat_inventory import seat_inventory
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/bookings", tags=["Bookings"])


@router.post(
    "",
    response_model=BookingResponse | BookingInitiateResponse,
    operation_id="create_booking",
    responses={
        201: {
            "model": BookingResponse,
            "description": "Booking created and confirmed",
            "links": {
                "ConfirmBooking": {
                    "operationId": "confirm_booking",
                    "parameters": {"booking_id": "$response.body#/booking_id"},
                },
                "GetBooking": {
                    "operationId": "get_booking",
                    "parameters": {"booking_id": "$response.body#/booking_id"},
                },
                "LookupBooking": {
                    "operationId": "lookup_booking",
                    "parameters": {"booking_code": "$response.body#/booking_code"},
                },
                "CancelBooking": {
                    "operationId": "cancel_booking",
                    "parameters": {"booking_id": "$response.body#/booking_id"},
                },
            },
        },
        202: {"model": BookingInitiateResponse, "description": "Booking accepted for asynchronous processing"},
        400: {"description": "Bad Request"},
        404: {"description": "Customer or flight seat not found"},
        409: {"description": "Seat Conflict"},
    },
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
    if payload.hold_token:
        hold_details = await seat_inventory.get_hold_details(payload.hold_token)
        if not hold_details:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "Seat Conflict",
                    "message": f"Hold token '{payload.hold_token}' is invalid or has expired",
                },
            )

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
    operation_id="lookup_booking",
    responses={
        200: {"model": BookingResponse, "description": "Booking details"},
        404: {"description": "Booking not found"},
    },
    summary="Lookup booking by PNR code and passenger last name (Cache-Aside)",
)
async def lookup_booking(
    booking_code: str = Query(..., min_length=4, max_length=16, pattern=r"^[A-Za-z0-9\-]+$", description="Booking confirmation code (e.g. BK-12345678)"),
    last_name: str | None = Query(None, min_length=1, max_length=60, pattern=r"^[^\x00]+$", description="Customer last name"),
) -> Response:
    """Lookup booking by PNR code and last name with Redis cache-aside.

    Uses lazy DB acquisition to bypass PgBouncer connection checkouts on cache hits.
    """
    norm_code = booking_code.strip().upper()
    norm_last_name = last_name.strip() if last_name else None
    cache_key = (
        f"cache:booking:lookup:{norm_code}:{norm_last_name.lower()}"
        if norm_last_name
        else f"cache:booking:lookup:{norm_code}"
    )

    async def fetch_from_db():
        from app.core.database import async_session_factory
        async with async_session_factory() as session:
            stmt = (
                select(Booking)
                .where(Booking.booking_code == norm_code)
                .options(selectinload(Booking.tickets))
            )
            if norm_last_name:
                stmt = stmt.join(Customer, Booking.customer_id == Customer.customer_id).where(
                    func.lower(Customer.last_name) == norm_last_name.lower()
                )
            res = await session.execute(stmt)
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
        detail_msg = (
            f"No booking found matching code {booking_code} and last name {last_name}"
            if last_name
            else f"No booking found matching code {booking_code}"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Not Found",
                "message": detail_msg,
            },
        )

    return Response(content=orjson.dumps(cached_data), media_type="application/json")


@router.get(
    "/{booking_id}",
    response_model=BookingResponse,
    operation_id="get_booking",
    responses={
        200: {"model": BookingResponse, "description": "Booking details"},
        404: {"description": "Booking not found"},
    },
    summary="Get booking details (Cache-Aside)",
)
async def get_booking(
    booking_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Booking ID"),
) -> Response:
    """Fetch booking details using cache-aside pattern."""
    cache_key = f"cache:booking:{booking_id}"

    async def fetch_from_db():
        from app.core.database import async_session_factory
        async with async_session_factory() as session:
            stmt = (
                select(Booking)
                .where(Booking.booking_id == booking_id)
                .options(selectinload(Booking.tickets))
            )
            res = await session.execute(stmt)
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

    return Response(content=orjson.dumps(cached_data), media_type="application/json")


@router.post(
    "/{booking_id}/confirm",
    response_model=BookingResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="confirm_booking",
    responses={
        201: {
            "model": BookingResponse,
            "description": "Booking confirmed and payment finalized",
            "links": {
                "GetBooking": {
                    "operationId": "get_booking",
                    "parameters": {"booking_id": "$response.body#/booking_id"},
                },
                "CancelBooking": {
                    "operationId": "cancel_booking",
                    "parameters": {"booking_id": "$response.body#/booking_id"},
                },
                "LookupBooking": {
                    "operationId": "lookup_booking",
                    "parameters": {"booking_code": "$response.body#/booking_code"},
                },
            },
        },
        400: {"description": "Bad Request"},
        404: {"description": "Booking not found"},
        409: {"description": "Seat Conflict"},
    },
    summary="Confirm and purchase a pending booking (via Redis Stream)",
)
async def confirm_booking(
    booking_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Booking ID"),
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
    operation_id="cancel_booking",
    responses={
        200: {"description": "Booking cancelled and seats released"},
        400: {"description": "Bad Request"},
        404: {"description": "Booking not found"},
    },
    summary="Cancel booking and release seats (via Redis Stream)",
)
async def cancel_booking(
    booking_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Booking ID"),
) -> dict:
    """Cancels booking and releases seats back to AVAILABLE via Redis Stream."""
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CANCEL_BOOKING,
        entity="booking",
        payload={"booking_id": booking_id},
    )
    return result
