from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.booking import Booking
    from app.models.flight_seat import FlightSeat


class Ticket(Base):
    __tablename__ = "ticket"

    ticket_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    booking_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("booking.booking_id", ondelete="CASCADE"),
        nullable=False,
    )
    flight_seat_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("flight_seat.flight_seat_id"),
        nullable=False,
    )
    passenger_name: Mapped[str] = mapped_column(String(100), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        server_default="ACTIVE",
        nullable=False,
    )
    ticket_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("price >= 0", name="chk_ticket_price"),
        CheckConstraint(
            "status IN ('ACTIVE', 'CANCELLED', 'REFUNDED')",
            name="chk_ticket_status",
        ),
        # Partial unique index: allows re-booking after cancellation while preventing double-booking
        Index(
            "uq_active_ticket_per_seat",
            "flight_seat_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
        Index("idx_ticket_booking_id", "booking_id"),
        {"postgresql_with": {"fillfactor": 90}},
    )

    # Relationships
    booking: Mapped["Booking"] = relationship(
        "Booking",
        back_populates="tickets",
    )
    flight_seat: Mapped["FlightSeat"] = relationship(
        "FlightSeat",
        back_populates="tickets",
    )
