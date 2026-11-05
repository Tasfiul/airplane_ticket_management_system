from typing import TYPE_CHECKING
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.flight import Flight
    from app.models.seat import Seat
    from app.models.ticket import Ticket


class FlightSeat(Base):
    __tablename__ = "flight_seat"

    flight_seat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    flight_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("flight.flight_id", ondelete="CASCADE"),
        nullable=False,
    )
    seat_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("seat.seat_id"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        server_default="AVAILABLE",
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("flight_id", "seat_id", name="uq_flight_seat"),
        CheckConstraint(
            "status IN ('AVAILABLE', 'BOOKED')",
            name="chk_flight_seat_status",
        ),
        Index("idx_flight_seat_seat_id", "seat_id"),
        {"postgresql_with": {"fillfactor": 80}},
    )

    # Relationships
    flight: Mapped["Flight"] = relationship(
        "Flight",
        back_populates="flight_seats",
    )
    seat: Mapped["Seat"] = relationship(
        "Seat",
        back_populates="flight_seats",
    )
    tickets: Mapped[list["Ticket"]] = relationship(
        "Ticket",
        back_populates="flight_seat",
    )
