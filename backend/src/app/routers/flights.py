import asyncio
from datetime import date, datetime, time, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import StreamingResponse
import orjson
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.core.redis import get_redis_client
from app.core.security import get_optional_current_customer
from app.models.flight import Flight
from app.models.flight_seat import FlightSeat
from app.schemas.booking import (
    SeatHoldRequest,
    SeatHoldResponse,
    SeatReleaseResponse,
)
from app.schemas.customer import CustomerResponse
from app.schemas.flight import (
    AirportInfo,
    FeaturedRouteResponse,
    FlightCreate,
    FlightResponse,
    FlightSearchResponse,
    FlightSeatResponse,
    FlightStatusUpdate,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.seat_inventory import seat_inventory
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/flights", tags=["Flights"])

AIRPORTS_METADATA: list[dict[str, str]] = [
    {"code": "JFK", "city": "New York", "name": "John F. Kennedy International Airport"},
    {"code": "LAX", "city": "Los Angeles", "name": "Los Angeles International Airport"},
    {"code": "ORD", "city": "Chicago", "name": "O'Hare International Airport"},
    {"code": "DFW", "city": "Dallas", "name": "Dallas/Fort Worth International Airport"},
    {"code": "DEN", "city": "Denver", "name": "Denver International Airport"},
    {"code": "SFO", "city": "San Francisco", "name": "San Francisco International Airport"},
    {"code": "SEA", "city": "Seattle", "name": "Seattle-Tacoma International Airport"},
    {"code": "ATL", "city": "Atlanta", "name": "Hartsfield-Jackson Atlanta International Airport"},
    {"code": "MIA", "city": "Miami", "name": "Miami International Airport"},
    {"code": "BOS", "city": "Boston", "name": "Logan International Airport"},
]

AIRPORT_CITY_MAP: dict[str, str] = {a["code"]: a["city"] for a in AIRPORTS_METADATA}


@router.get(
    "/search",
    response_model=FlightSearchResponse,
    operation_id="search_flights",
    responses={
        200: {"model": FlightSearchResponse, "description": "Flight search results"},
        400: {"description": "Bad Request"},
    },
    summary="Search scheduled flights by route and date (Covering Index-Only Scan)",
)
async def search_flights(
    origin: str = Query(..., min_length=3, max_length=3, pattern="^[A-Za-z]{3}$", description="Departure airport 3-letter IATA code", examples=["JFK"]),
    destination: str = Query(..., min_length=3, max_length=3, pattern="^[A-Za-z]{3}$", description="Arrival airport 3-letter IATA code", examples=["LAX"]),
    departure_date: date | None = Query(None, description="Optional flight departure date (YYYY-MM-DD)", examples=["2026-12-01"]),
    page: int = Query(1, ge=1, le=2_147_483_647, description="Page number (1-based)"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page (max 100)"),
    db: AsyncSession = Depends(get_db),
) -> FlightSearchResponse:
    """High-performance route search following the Cache-Aside pattern.

    Cache misses query PostgreSQL through PgBouncer using composite covering index
    `idx_flight_route_covering` (origin, destination, departure_time) INCLUDE (...)
    serving queries via Index-Only Scan in 2-5 ms.
    """
    origin_u = origin.upper()
    dest_u = destination.upper()
    if origin_u == dest_u:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad Request", "message": "Origin and destination airports must be different"},
        )

    date_str = departure_date.isoformat() if departure_date else "any"
    cache_key = f"cache:search:{origin_u}:{dest_u}:{date_str}:p{page}:s{page_size}"

    async def fetch_search_results():
        conditions = [
            Flight.departure_airport == origin_u,
            Flight.arrival_airport == dest_u,
            Flight.status == "SCHEDULED",
        ]
        if departure_date:
            start_dt = datetime.combine(departure_date, time.min).replace(tzinfo=timezone.utc)
            end_dt = datetime.combine(departure_date, time.max).replace(tzinfo=timezone.utc)
            conditions.append(Flight.departure_time >= start_dt)
            conditions.append(Flight.departure_time <= end_dt)
        else:
            now_dt = datetime.now(timezone.utc)
            conditions.append(Flight.departure_time >= now_dt)

        count_stmt = select(func.count(Flight.flight_id)).where(*conditions)
        total_res = await db.execute(count_stmt)
        total_count = total_res.scalar() or 0

        data_stmt = (
            select(Flight)
            .where(*conditions)
            .order_by(Flight.departure_time.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        data_res = await db.execute(data_stmt)
        flights = data_res.scalars().all()

        return {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "results": [
                FlightResponse.model_validate(f).model_dump(mode="json")
                for f in flights
            ],
        }

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_search_results,
        ttl=300,
    )
    return FlightSearchResponse.model_validate(cached_data)


@router.get(
    "/airports",
    response_model=list[AirportInfo],
    operation_id="get_airports",
    summary="List supported airports and cities for frontend search autocomplete",
)
async def get_airports() -> list[AirportInfo]:
    """Returns airport metadata cached in Redis for fast frontend autocomplete."""
    cache_key = "cache:meta:airports"

    async def fetch_airports():
        return AIRPORTS_METADATA

    data = await cache_service.get_or_set(cache_key, fetch_airports, ttl=86400)
    return [AirportInfo.model_validate(a) for a in data]


@router.get(
    "/featured-routes",
    response_model=list[FeaturedRouteResponse],
    operation_id="get_featured_routes",
    summary="List popular featured routes for home screen discovery",
)
async def get_featured_routes() -> list[FeaturedRouteResponse]:
    """Returns top featured routes cached in Redis."""
    cache_key = "cache:meta:featured_routes"

    featured = [
        {"origin": "JFK", "destination": "LAX", "daily_flights_count": 8},
        {"origin": "LAX", "destination": "JFK", "daily_flights_count": 8},
        {"origin": "ORD", "destination": "DFW", "daily_flights_count": 6},
        {"origin": "SFO", "destination": "SEA", "daily_flights_count": 5},
        {"origin": "ATL", "destination": "MIA", "daily_flights_count": 7},
    ]

    async def fetch_featured():
        for r in featured:
            r["origin_city"] = AIRPORT_CITY_MAP.get(r["origin"], r["origin"])
            r["destination_city"] = AIRPORT_CITY_MAP.get(r["destination"], r["destination"])
        return featured

    data = await cache_service.get_or_set(cache_key, fetch_featured, ttl=3600)
    return [FeaturedRouteResponse.model_validate(r) for r in data]


@router.post(
    "",
    response_model=FlightResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_flight",
    responses={
        201: {
            "model": FlightResponse,
            "description": "Flight scheduled successfully",
            "links": {
                "GetFlight": {
                    "operationId": "get_flight",
                    "parameters": {"flight_id": "$response.body#/flight_id"},
                },
                "GetFlightSeats": {
                    "operationId": "get_flight_seats",
                    "parameters": {"flight_id": "$response.body#/flight_id"},
                },
                "HoldFlightSeats": {
                    "operationId": "hold_flight_seats",
                    "parameters": {"flight_id": "$response.body#/flight_id"},
                },
            },
        },
        400: {"description": "Bad Request"},
        404: {"description": "Plane not found"},
        409: {"description": "Conflict"},
    },
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
    operation_id="get_flight",
    responses={
        200: {
            "model": FlightResponse,
            "description": "Flight details",
            "links": {
                "GetFlightSeats": {
                    "operationId": "get_flight_seats",
                    "parameters": {"flight_id": "$response.body#/flight_id"},
                },
                "HoldFlightSeats": {
                    "operationId": "hold_flight_seats",
                    "parameters": {"flight_id": "$response.body#/flight_id"},
                },
            },
        },
        404: {"description": "Flight not found"},
    },
    summary="Get flight details (Cache-Aside)",
)
async def get_flight(
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    db: AsyncSession = Depends(get_db),
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
    operation_id="get_flight_seats",
    responses={
        200: {"model": list[FlightSeatResponse], "description": "Flight seat availability"},
        404: {"description": "Flight not found"},
    },
    summary="Get flight seat availability (Redis Hash Cache-Aside)",
)
async def get_flight_seats(
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    db: AsyncSession = Depends(get_db),
) -> list[FlightSeatResponse]:
    """Fetch seat map for flight directly from Redis Hash in sub-millisecond."""
    seats_data = await seat_inventory.get_or_load_seat_map(flight_id, db, ttl=1800)
    return [FlightSeatResponse.model_validate(s) for s in seats_data]


@router.post(
    "/{flight_id}/seats/hold",
    response_model=SeatHoldResponse,
    operation_id="hold_flight_seats",
    responses={
        200: {
            "model": SeatHoldResponse,
            "description": "Seats held successfully for checkout",
            "links": {
                "ReleaseSeats": {
                    "operationId": "release_flight_seats",
                    "parameters": {
                        "flight_id": "$response.body#/flight_id",
                        "hold_token": "$response.body#/hold_token",
                    },
                },
            },
        },
        400: {"description": "Bad Request"},
        404: {"description": "Flight Not Found"},
        409: {"description": "Seat Conflict - seats already booked or held"},
    },
    summary="Hold flight seats temporarily during checkout (Redis Atomic Lua)",
)
async def hold_flight_seats(
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    payload: SeatHoldRequest = ...,
    current_customer: CustomerResponse | None = Depends(get_optional_current_customer),
    db: AsyncSession = Depends(get_db),
) -> SeatHoldResponse:
    """Atomically places an ephemeral hold on seats via Redis Lua script with a TTL.

    No database writes are executed. Returns a hold token required for checkout.
    If any requested seat is already booked or held, returns 409 Conflict.
    """
    # Verify flight exists in DB/Redis
    stmt = select(Flight.flight_id).where(Flight.flight_id == flight_id)
    res = await db.execute(stmt)
    if not res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Flight {flight_id} not found"},
        )

    customer_id = current_customer.customer_id if current_customer else 0
    ok, hold_token, expires_at = await seat_inventory.hold_seats_atomic(
        flight_id=flight_id,
        flight_seat_ids=payload.flight_seat_ids,
        customer_id=customer_id,
        ttl_seconds=payload.hold_duration_seconds,
        db_fallback=db,
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "Seat Conflict",
                "message": "One or more requested seats are already booked or held by another customer",
            },
        )
    return SeatHoldResponse(
        hold_token=hold_token,
        flight_id=flight_id,
        flight_seat_ids=payload.flight_seat_ids,
        expires_at=expires_at,
        hold_duration_seconds=payload.hold_duration_seconds,
        message="Seats held successfully for checkout",
    )


@router.post(
    "/{flight_id}/seats/release",
    response_model=SeatReleaseResponse,
    operation_id="release_flight_seats",
    responses={
        200: {
            "model": SeatReleaseResponse,
            "description": "Seats released successfully",
        },
        404: {"description": "Hold token not found or already expired"},
    },
    summary="Release held seats back to available (Redis Atomic Lua)",
)
async def release_flight_seats(
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    hold_token: str = Query(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9\-]+$", description="The hold token issued during seat hold"),
) -> SeatReleaseResponse:
    """Atomically releases held seats back to AVAILABLE in Redis."""
    released = await seat_inventory.release_hold_atomic(hold_token)
    if not released:
        return SeatReleaseResponse(
            released=False,
            message=f"Hold token '{hold_token}' not found or already expired",
        )
    return SeatReleaseResponse(
        released=True,
        message="Seats released successfully",
    )


@router.patch(
    "/{flight_id}/status",
    operation_id="update_flight_status",
    responses={
        200: {"description": "Flight status updated successfully"},
        400: {"description": "Bad Request"},
        404: {"description": "Flight not found"},
    },
    summary="Update flight status (via Redis Stream)",
)
async def update_flight_status(
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    payload: FlightStatusUpdate = ...,
) -> dict:
    """Update flight status strictly via Redis Stream write pipeline."""
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.UPDATE_FLIGHT_STATUS,
        entity="flight",
        payload={"flight_id": flight_id, "status": payload.status},
    )
    return result


@router.get(
    "/{flight_id}/seats/stream",
    response_class=StreamingResponse,
    operation_id="stream_flight_seats",
    summary="Real-time Server-Sent Events (SSE) stream for live seat status changes",
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "Real-time SSE event stream for seat availability changes",
        },
        404: {"description": "Flight not found"},
    },
)
async def stream_flight_seats(
    request: Request,
    flight_id: int = Path(..., ge=1, le=9_223_372_036_854_775_807, description="Flight ID", examples=[1]),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    """Streams real-time seat status updates (HELD, BOOKED, AVAILABLE) using Server-Sent Events (SSE).

    Backed by Redis Pub/Sub channel `flight:{flight_id}:seats:events`.
    Emits initial connection handshake event, real-time diffs, and periodic keepalive comments.
    """
    stmt = select(Flight.flight_id).where(Flight.flight_id == flight_id)
    res = await db.execute(stmt)
    if not res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Flight {flight_id} not found"},
        )

    channel_name = f"flight:{flight_id}:seats:events"

    async def event_generator():
        redis = get_redis_client()
        pubsub = redis.pubsub()
        await pubsub.subscribe(channel_name)

        try:
            connect_payload = orjson.dumps({
                "status": "connected",
                "flight_id": flight_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }).decode("utf-8")
            yield f"event: connect\ndata: {connect_payload}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                try:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=15.0
                    )
                    if msg and msg["type"] == "message":
                        data_val = msg["data"]
                        if isinstance(data_val, bytes):
                            data_val = data_val.decode("utf-8")
                        yield f"event: seat_update\ndata: {data_val}\n\n"
                    else:
                        yield ": keepalive\n\n"
                except asyncio.CancelledError:
                    break
                except Exception:
                    break
        finally:
            try:
                await pubsub.unsubscribe(channel_name)
                await pubsub.close()
            except Exception:
                pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
