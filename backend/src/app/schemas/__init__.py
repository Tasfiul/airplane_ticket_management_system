from app.schemas.auth import LoginRequest, TokenResponse
from app.schemas.booking import (
    BookingCreate,
    BookingResponse,
    TicketItemCreate,
    TicketResponse,
)
from app.schemas.customer import CustomerCreate, CustomerResponse, CustomerUpdate
from app.schemas.flight import (
    AirportInfo,
    FeaturedRouteResponse,
    FlightCreate,
    FlightResponse,
    FlightSearchResponse,
    FlightSeatResponse,
    FlightStatusUpdate,
)
from app.schemas.write_event import (
    WriteAction,
    WriteEvent,
    WriteResult,
    WriteResultStatus,
)

__all__ = [
    "CustomerCreate",
    "CustomerResponse",
    "CustomerUpdate",
    "LoginRequest",
    "TokenResponse",
    "FlightCreate",
    "FlightResponse",
    "FlightSearchResponse",
    "FlightSeatResponse",
    "FlightStatusUpdate",
    "AirportInfo",
    "FeaturedRouteResponse",
    "TicketItemCreate",
    "BookingCreate",
    "TicketResponse",
    "BookingResponse",
    "WriteAction",
    "WriteEvent",
    "WriteResult",
    "WriteResultStatus",
]
