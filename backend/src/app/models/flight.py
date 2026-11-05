from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.flight_seat import FlightSeat
    from app.models.plane import Plane


class Flight(Base):
    __tablename__ = "flight"

    flight_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    flight_number: Mapped[str] = mapped_column(String(10), nullable=False)
    plane_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("plane.plane_id"),
        nullable=False,
    )
    departure_airport: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    arrival_airport: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    arrival_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        server_default="SCHEDULED",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("arrival_time > departure_time", name="chk_flight_times"),
        CheckConstraint("departure_airport <> arrival_airport", name="chk_flight_airports"),
        CheckConstraint(
            "status IN ('SCHEDULED', 'DELAYED', 'DEPARTED', 'CANCELLED')",
            name="chk_flight_status",
        ),
        Index("idx_flight_search", "departure_airport", "arrival_airport", "departure_time"),
        Index(
            "idx_flight_route_covering",
            "departure_airport",
            "arrival_airport",
            "departure_time",
            postgresql_include=["flight_id", "flight_number", "plane_id", "status"],
        ),
        Index("idx_flight_plane_id", "plane_id"),
        {"postgresql_with": {"fillfactor": 90}},
    )

    # Relationships
    plane: Mapped["Plane"] = relationship(
        "Plane",
        back_populates="flights",
    )
    flight_seats: Mapped[list["FlightSeat"]] = relationship(
        "FlightSeat",
        back_populates="flight",
        cascade="all, delete-orphan",
    )
