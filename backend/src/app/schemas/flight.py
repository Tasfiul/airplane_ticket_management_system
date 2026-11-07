from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class FlightCreate(BaseModel):
    flight_number: str = Field(..., min_length=2, max_length=10)
    plane_id: int
    departure_airport: str = Field(..., min_length=3, max_length=3)
    arrival_airport: str = Field(..., min_length=3, max_length=3)
    departure_time: datetime
    arrival_time: datetime
    status: str = Field(default="SCHEDULED")


class FlightSeatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flight_seat_id: int
    flight_id: int
    seat_id: int
    status: str
    seat_number: str | None = None
    seat_class: str | None = None


class FlightResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flight_id: int
    flight_number: str
    plane_id: int
    departure_airport: str
    arrival_airport: str
    departure_time: datetime
    arrival_time: datetime
    status: str
    created_at: datetime


class FlightStatusUpdate(BaseModel):
    status: str = Field(..., pattern="^(SCHEDULED|DELAYED|DEPARTED|CANCELLED)$")


class AirportInfo(BaseModel):
    code: str
    city: str
    name: str


class FlightSearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[FlightResponse]


class FeaturedRouteResponse(BaseModel):
    origin: str
    destination: str
    origin_city: str
    destination_city: str
    daily_flights_count: int
