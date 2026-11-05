import datetime
from decimal import Decimal
import pytest
from sqlalchemy import select
from app.core.database import async_session_factory
from app.models import (
    Booking,
    BookingStatus,
    Customer,
    Flight,
    FlightSeat,
    FlightSeatStatus,
    FlightStatus,
    Plane,
    Seat,
    SeatClass,
    Ticket,
    TicketStatus,
)


@pytest.mark.asyncio
async def test_create_and_query_entities():
    async with async_session_factory() as session:
        # Create Customer
        customer = Customer(
            first_name="Jane",
            last_name="Doe",
            email="jane.doe@example.com",
            password_hash="hashed_pw_test",
            city="Dhaka",
            postal_code="1200",
            division="Dhaka",
            country="Bangladesh",
            date_of_birth=datetime.date(1995, 5, 20),
        )
        session.add(customer)

        # Create Plane
        plane = Plane(
            tail_number="S2-ABC",
            model="Boeing 787-9",
            total_seats=2,
        )
        session.add(plane)
        await session.flush()

        # Create Seat
        seat = Seat(
            plane_id=plane.plane_id,
            seat_number="1A",
            seat_class=SeatClass.BUSINESS,
        )
        session.add(seat)
        await session.flush()

        # Create Flight
        now = datetime.datetime.now(datetime.timezone.utc)
        flight = Flight(
            flight_number="BG001",
            plane_id=plane.plane_id,
            departure_airport="DAC",
            arrival_airport="JFK",
            departure_time=now + datetime.timedelta(days=1),
            arrival_time=now + datetime.timedelta(days=1, hours=14),
            status=FlightStatus.SCHEDULED,
        )
        session.add(flight)
        await session.flush()

        # Create FlightSeat
        flight_seat = FlightSeat(
            flight_id=flight.flight_id,
            seat_id=seat.seat_id,
            status=FlightSeatStatus.BOOKED,
        )
        session.add(flight_seat)
        await session.flush()

        # Create Booking
        booking = Booking(
            booking_code="BK123456",
            customer_id=customer.customer_id,
            total_amount=Decimal("1250.00"),
            status=BookingStatus.PAID,
        )
        session.add(booking)
        await session.flush()

        # Create Ticket
        ticket = Ticket(
            booking_id=booking.booking_id,
            flight_seat_id=flight_seat.flight_seat_id,
            passenger_name="Jane Doe",
            price=Decimal("1250.00"),
            status=TicketStatus.ACTIVE,
        )
        session.add(ticket)
        await session.commit()

        # Query and verify
        res = await session.execute(
            select(Ticket).where(Ticket.passenger_name == "Jane Doe")
        )
        fetched_ticket = res.scalar_one_or_none()
        assert fetched_ticket is not None
        assert fetched_ticket.price == Decimal("1250.00")
        assert fetched_ticket.status == TicketStatus.ACTIVE

        # Clean up test data
        await session.delete(booking)  # cascades to ticket
        await session.delete(flight)   # cascades to flight_seat
        await session.delete(plane)    # cascades to seat
        await session.delete(customer)
        await session.commit()
