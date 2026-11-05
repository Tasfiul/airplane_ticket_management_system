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
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

if TYPE_CHECKING:
    from app.models.customer import Customer
    from app.models.ticket import Ticket


class Booking(Base):
    __tablename__ = "booking"

    booking_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    booking_code: Mapped[str] = mapped_column(String(12), unique=True, nullable=False)
    customer_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("customer.customer_id"),
        nullable=False,
    )
    total_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        server_default="PENDING",
        nullable=False,
    )
    receipt_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("total_amount >= 0", name="chk_booking_total_amount"),
        CheckConstraint(
            "status IN ('PENDING', 'PAID', 'CANCELLED', 'REFUNDED')",
            name="chk_booking_status",
        ),
        Index("idx_booking_customer_id", "customer_id"),
        {"postgresql_with": {"fillfactor": 85}},
    )

    # Relationships
    customer: Mapped["Customer"] = relationship(
        "Customer",
        back_populates="bookings",
    )
    tickets: Mapped[list["Ticket"]] = relationship(
        "Ticket",
        back_populates="booking",
        cascade="all, delete-orphan",
    )
