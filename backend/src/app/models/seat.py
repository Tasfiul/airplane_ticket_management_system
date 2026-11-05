from typing import TYPE_CHECKING
from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.flight_seat import FlightSeat
    from app.models.plane import Plane


class Seat(Base):
    __tablename__ = "seat"

    seat_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plane_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("plane.plane_id", ondelete="CASCADE"),
        nullable=False,
    )
    seat_number: Mapped[str] = mapped_column(String(5), nullable=False)
    seat_class: Mapped[str] = mapped_column(String(20), nullable=False)

    __table_args__ = (
        UniqueConstraint("plane_id", "seat_number", name="uq_plane_seat"),
        CheckConstraint(
            "seat_class IN ('ECONOMY', 'PREMIUM_ECONOMY', 'BUSINESS', 'FIRST')",
            name="chk_seat_class",
        ),
    )

    # Relationships
    plane: Mapped["Plane"] = relationship(
        "Plane",
        back_populates="seats",
    )
    flight_seats: Mapped[list["FlightSeat"]] = relationship(
        "FlightSeat",
        back_populates="seat",
    )
