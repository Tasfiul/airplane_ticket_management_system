from datetime import date, datetime, time, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.flight import Flight
from app.models.flight_seat import FlightSeat
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
    summary="Search scheduled flights by route and date (Covering Index-Only Scan)",
)
async def search_flights(
    origin: str = Query(..., min_length=3, max_length=3, description="Departure airport 3-letter IATA code"),
    destination: str = Query(..., min_length=3, max_length=3, description="Arrival airport 3-letter IATA code"),
    departure_date: date | None = Query(None, description="Optional flight departure date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
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
            detail={"error": "Bad Request", "message": "Departure and arrival airports must be different"},
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
