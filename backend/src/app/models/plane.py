from typing import TYPE_CHECKING
from sqlalchemy import CheckConstraint, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.flight import Flight
    from app.models.seat import Seat


class Plane(Base):
    __tablename__ = "plane"

    plane_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tail_number: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    model: Mapped[str] = mapped_column(String(50), nullable=False)
    total_seats: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        CheckConstraint("total_seats > 0", name="chk_plane_total_seats"),
    )

    # Relationships
    seats: Mapped[list["Seat"]] = relationship(
        "Seat",
        back_populates="plane",
        cascade="all, delete-orphan",
    )
    flights: Mapped[list["Flight"]] = relationship(
        "Flight",
        back_populates="plane",
    )
