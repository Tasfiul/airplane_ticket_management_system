import asyncio
import uuid
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from app.core.database import async_session_factory
from app.core.redis import close_redis_pool, get_redis_client
from app.main import app, lifespan
from app.models.customer import Customer
from app.models.flight import Flight
from app.models.flight_seat import FlightSeat
from app.services.stream_worker import stream_worker


@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_phase3_seat_hold_conflict_and_release():
    """Test ephemeral seat holding, conflict detection (409), and release."""
    await stream_worker.init_consumer_group()
    worker_task = asyncio.create_task(stream_worker.run_loop())

    try:
        async with lifespan(app):
            async with async_session_factory() as db:
                # Find a scheduled flight with available seats
                res = await db.execute(
                    select(Flight.flight_id)
                    .join(FlightSeat, Flight.flight_id == FlightSeat.flight_id)
                    .where(FlightSeat.status == "AVAILABLE")
                    .limit(1)
                )
                flight_id = res.scalar_one_or_none()
                assert flight_id is not None, "A flight with available seats must exist"

                seat_res = await db.execute(
                    select(FlightSeat.flight_seat_id)
                    .where(FlightSeat.flight_id == flight_id, FlightSeat.status == "AVAILABLE")
                    .limit(2)
                )
                seat_ids = list(seat_res.scalars().all())
                assert len(seat_ids) >= 1, "At least one available seat must exist"

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                # 1. Hold the seat
                hold_resp = await client.post(
                    f"/api/v1/flights/{flight_id}/seats/hold",
                    json={"flight_seat_ids": [seat_ids[0]], "hold_duration_seconds": 300},
                )
                assert hold_resp.status_code == 200, f"Seat hold failed: {hold_resp.text}"
                hold_data = hold_resp.json()
                hold_token = hold_data["hold_token"]
                assert hold_token is not None
                assert hold_data["flight_seat_ids"] == [seat_ids[0]]
                assert hold_data["hold_duration_seconds"] == 300

                # Verify Redis state
                redis = get_redis_client()
                held_val = await redis.get(f"seat_held:{flight_id}:{seat_ids[0]}")
                assert held_val == hold_token or held_val == hold_token.encode("utf-8")

                # 2. Conflicting hold attempt should return 409 Conflict
                conflict_resp = await client.post(
                    f"/api/v1/flights/{flight_id}/seats/hold",
                    json={"flight_seat_ids": [seat_ids[0]], "hold_duration_seconds": 300},
                )
                assert conflict_resp.status_code == 409, "Second hold on same seat must return 409 Conflict"

                # 3. Release the hold
                release_resp = await client.post(
                    f"/api/v1/flights/{flight_id}/seats/release?hold_token={hold_token}",
                )
                assert release_resp.status_code == 200, f"Seat release failed: {release_resp.text}"
                assert release_resp.json()["released"] is True

                # Verify Redis hold key deleted
                post_held = await redis.get(f"seat_held:{flight_id}:{seat_ids[0]}")
                assert post_held is None
    finally:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_phase3_booking_async_and_confirm_lifecycle():
    """Test full Phase 3 booking lifecycle:

    1. Async ingestion with 202 Accepted.
    2. Synchronous booking creation with hold token.
    3. Confirmation (201 Created) transitioning PENDING to PAID.
    4. PNR booking lookup by code and passenger last name.
    5. Rejection of confirming a cancelled booking.
    """
    await stream_worker.init_consumer_group()
    worker_task = asyncio.create_task(stream_worker.run_loop())

    try:
        async with lifespan(app):
            async with async_session_factory() as db:
                cust_res = await db.execute(
                    select(Customer.customer_id, Customer.last_name).limit(1)
                )
                cust_row = cust_res.first()
                assert cust_row is not None
                customer_id, customer_last_name = cust_row

                seat_res = await db.execute(
                    select(FlightSeat.flight_seat_id, FlightSeat.flight_id)
                    .where(FlightSeat.status == "AVAILABLE")
                    .limit(3)
                )
                seat_rows = seat_res.all()
                assert len(seat_rows) >= 2

            seat1_id, flight1_id = seat_rows[0]
            seat2_id, flight2_id = seat_rows[1]

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                # 1. Test 202 Accepted fast-path async ingest
                async_resp = await client.post(
                    "/api/v1/bookings?async_ingest=true",
                    json={
                        "customer_id": customer_id,
                        "tickets": [
                            {
                                "flight_seat_id": seat1_id,
                                "passenger_name": "Async Passenger",
                                "price": "149.99",
                            }
                        ],
                    },
                )
                assert async_resp.status_code == 202, f"Async booking failed: {async_resp.text}"
                async_data = async_resp.json()
                assert async_data["status"] == "PENDING"
                assert "event_id" in async_data

                # 2. Hold seat2
                hold_resp = await client.post(
                    f"/api/v1/flights/{flight2_id}/seats/hold",
                    json={"flight_seat_ids": [seat2_id], "hold_duration_seconds": 600},
                )
                assert hold_resp.status_code == 200
                hold_token = hold_resp.json()["hold_token"]

                # 3. Create booking in PENDING status with hold_token
                book_resp = await client.post(
                    "/api/v1/bookings",
                    json={
                        "customer_id": customer_id,
                        "hold_token": hold_token,
                        "status": "PENDING",
                        "tickets": [
                            {
                                "flight_seat_id": seat2_id,
                                "passenger_name": "Jane Test Passenger",
                                "price": "349.50",
                            }
                        ],
                    },
                )
                assert book_resp.status_code == 201, f"Booking creation failed: {book_resp.text}"
                booking_data = book_resp.json()
                booking_id = booking_data["booking_id"]
                booking_code = booking_data["booking_code"]
                assert booking_data["status"] == "PENDING"

                # 4. Confirm booking -> transitions PENDING to PAID (201 Created)
                confirm_resp = await client.post(
                    f"/api/v1/bookings/{booking_id}/confirm",
                    json={"payment_method": "CREDIT_CARD", "hold_token": hold_token},
                )
                assert confirm_resp.status_code == 201, f"Confirm booking failed: {confirm_resp.text}"
                confirmed_data = confirm_resp.json()
                assert confirmed_data["booking_id"] == booking_id
                assert confirmed_data["status"] == "PAID"
                assert len(confirmed_data["tickets"]) == 1
                assert confirmed_data["tickets"][0]["status"] == "ACTIVE"

                # 5. Lookup booking by PNR and customer last name
                lookup_resp = await client.get(
                    f"/api/v1/bookings/lookup?booking_code={booking_code}&last_name={customer_last_name}"
                )
                assert lookup_resp.status_code == 200, f"Booking lookup failed: {lookup_resp.text}"
                lookup_data = lookup_resp.json()
                assert lookup_data["booking_id"] == booking_id
                assert lookup_data["booking_code"] == booking_code
                assert lookup_data["status"] == "PAID"

                # 6. Lookup with wrong last name should return 404
                wrong_lookup = await client.get(
                    f"/api/v1/bookings/lookup?booking_code={booking_code}&last_name=NonExistentLastNameXYZ"
                )
                assert wrong_lookup.status_code == 404

                # 7. Cancel the booking
                cancel_resp = await client.post(f"/api/v1/bookings/{booking_id}/cancel")
                assert cancel_resp.status_code == 200

                # 8. Attempting to confirm a cancelled booking must return 400
                bad_confirm = await client.post(f"/api/v1/bookings/{booking_id}/confirm")
                assert bad_confirm.status_code == 400
    finally:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
