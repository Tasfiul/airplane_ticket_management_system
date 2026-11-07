from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, model_validator


class FlightCreate(BaseModel):
    flight_number: str = Field(..., min_length=2, max_length=10, pattern=r"^[A-Za-z0-9\-]+$", examples=["AA123"])
    plane_id: int = Field(..., ge=1, le=100, examples=[1])
    departure_airport: str = Field(..., min_length=3, max_length=3, pattern=r"^[A-Za-z]{3}$", examples=["JFK"])
    arrival_airport: str = Field(..., min_length=3, max_length=3, pattern=r"^[A-Za-z]{3}$", examples=["LAX"])
    departure_time: datetime = Field(..., examples=["2026-12-01T10:00:00Z"])
    arrival_time: datetime = Field(..., examples=["2026-12-01T14:00:00Z"])
    status: str = Field(default="SCHEDULED", pattern="^(SCHEDULED|DELAYED|DEPARTED|CANCELLED)$")

    @model_validator(mode="after")
    def validate_flight_airports_and_times(self) -> "FlightCreate":
        if self.departure_airport.upper() == self.arrival_airport.upper():
            raise ValueError("Departure airport and arrival airport must be different")
        if self.arrival_time <= self.departure_time:
            raise ValueError("Arrival time must be after departure time")
        return self


class FlightSeatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flight_seat_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    flight_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    seat_id: int = Field(..., ge=1, le=2_147_483_647)
    status: str
    seat_number: str | None = None
    seat_class: str | None = None


class FlightResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flight_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    flight_number: str
    plane_id: int = Field(..., ge=1, le=2_147_483_647)
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
