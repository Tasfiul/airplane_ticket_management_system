from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field


class TicketItemCreate(BaseModel):
    flight_seat_id: int
    passenger_name: str = Field(..., min_length=1, max_length=100)
    price: Decimal = Field(..., ge=0)


class BookingCreate(BaseModel):
    customer_id: int
    tickets: list[TicketItemCreate] = Field(..., min_length=1)
    hold_token: str | None = None
    status: str | None = None


class TicketResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticket_id: int
    booking_id: int
    flight_seat_id: int
    passenger_name: str
    price: Decimal
    status: str
    ticket_url: str | None = None
    issued_at: datetime


class BookingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    booking_id: int
    booking_code: str
    customer_id: int
    total_amount: Decimal
    status: str
    receipt_url: str | None = None
    created_at: datetime
    tickets: list[TicketResponse] = []


class SeatHoldRequest(BaseModel):
    flight_seat_ids: list[int] = Field(..., min_length=1)
    hold_duration_seconds: int = Field(default=600, ge=60, le=1800)


class SeatHoldResponse(BaseModel):
    hold_token: str
    flight_id: int
    flight_seat_ids: list[int]
    expires_at: datetime
    hold_duration_seconds: int
    message: str = "Seats held successfully for checkout"


class SeatReleaseResponse(BaseModel):
    released: bool
    message: str


class BookingInitiateResponse(BaseModel):
    event_id: str
    status: str = "PENDING"
    message: str = "Booking initiated and queued for asynchronous ingestion"


class BookingConfirmRequest(BaseModel):
    payment_method: str = Field(default="CREDIT_CARD")
    hold_token: str | None = None
