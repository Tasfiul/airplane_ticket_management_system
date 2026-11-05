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
