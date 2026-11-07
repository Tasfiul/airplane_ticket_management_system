from datetime import datetime
from decimal import Decimal
from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field, model_validator


class TicketItemCreate(BaseModel):
    flight_seat_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807, examples=[1])
    passenger_name: str = Field(..., min_length=1, max_length=100, pattern=r"^[^\x00]+$", examples=["Alice Smith"])
    price: Decimal = Field(..., ge=0, le=1_000_000, decimal_places=2, examples=[Decimal("150.00")])


class BookingCreate(BaseModel):
    customer_id: int = Field(..., ge=1, le=2_147_483_647, examples=[1])
    tickets: list[TicketItemCreate] = Field(..., min_length=1, max_length=50)
    hold_token: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9\-]*$")
    status: str | None = Field(default=None, pattern="^(PENDING|PAID|CANCELLED|EXPIRED)$")


class TicketResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticket_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    booking_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    flight_seat_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    passenger_name: str
    price: Decimal
    status: str
    ticket_url: str | None = None
    issued_at: datetime

    @model_validator(mode="after")
    def populate_ticket_url(self) -> "TicketResponse":
        if not self.ticket_url and self.ticket_id:
            self.ticket_url = f"/api/v1/tickets/{self.ticket_id}/pdf"
        return self


class BookingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    booking_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
    booking_code: str
    customer_id: int = Field(..., ge=1, le=2_147_483_647)
    total_amount: Decimal
    status: str
    receipt_url: str | None = None
    created_at: datetime
    tickets: list[TicketResponse] = []


class SeatHoldRequest(BaseModel):
    flight_seat_ids: list[Annotated[int, Field(ge=1, le=9_223_372_036_854_775_807)]] = Field(
        ..., min_length=1, max_length=10, examples=[[1]]
    )
    hold_duration_seconds: int = Field(default=600, ge=60, le=1800, examples=[600])


class SeatHoldResponse(BaseModel):
    hold_token: str
    flight_id: int = Field(..., ge=1, le=9_223_372_036_854_775_807)
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
    payment_method: str = Field(default="CREDIT_CARD", pattern=r"^[A-Za-z0-9_\-]+$")
    hold_token: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9\-]*$")
