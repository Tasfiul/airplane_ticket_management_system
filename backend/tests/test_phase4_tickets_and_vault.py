import asyncio
import io
import uuid
import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from app.core.database import async_session_factory
from app.core.redis import close_redis_pool
from app.main import app, lifespan
from app.models.customer import Customer
from app.models.flight_seat import FlightSeat
from app.models.ticket import Ticket
from app.services.minio_storage import minio_service
from app.services.stream_worker import stream_worker


@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_ticket_pdf_in_memory_rendering_and_upload():
    """Verify in-memory PDF generation, MinIO upload, and presigned URL download."""
    ticket_data = {
        "booking_code": "BK-UNITTEST",
        "ticket_id": 12345,
        "passenger_name": "Test Passenger Alpha",
        "flight_number": "FL-777",
        "departure_airport": "JFK",
        "arrival_airport": "LAX",
        "price": "249.99",
        "status": "ACTIVE",
        "seat_number": "14B",
    }

    # 1. Generate in-memory PDF
    pdf_bytes = minio_service.generate_boarding_pass_pdf(ticket_data)
    assert len(pdf_bytes) > 500, "Boarding pass PDF must have valid content"
    assert pdf_bytes.startswith(b"%PDF"), "Must be a valid PDF format"

    # 2. Upload to MinIO
    object_key = minio_service.upload_ticket_pdf("BK-UNITTEST", 12345, pdf_bytes)
    assert object_key == "tickets/BK-UNITTEST/12345.pdf"
    assert minio_service.object_exists(object_key) is True

    # 3. Generate presigned download URL
    download_url = minio_service.get_presigned_download_url(object_key, expires_seconds=300)
    assert "X-Amz-Signature" in download_url
    assert "tickets" in download_url

    # 4. Fetch bytes directly from MinIO using HTTP client
    async with httpx.AsyncClient() as client:
        resp = await client.get(download_url)
        assert resp.status_code == 200
        assert resp.headers.get("content-type") == "application/pdf"
        assert resp.content == pdf_bytes


@pytest.mark.asyncio
async def test_ticket_endpoints_and_pdf_redirect():
    """Verify GET /api/v1/tickets/{id} and GET /api/v1/tickets/{id}/pdf (307 redirect)."""
    await stream_worker.init_consumer_group()
    worker_task = asyncio.create_task(stream_worker.run_loop())

    try:
        async with lifespan(app):
            # 1. Purchase a ticket through the booking pipeline
            async with async_session_factory() as db:
                cust_res = await db.execute(select(Customer.customer_id).limit(1))
                customer_id = cust_res.scalar_one_or_none()
                assert customer_id is not None

                seat_res = await db.execute(
                    select(FlightSeat.flight_seat_id)
                    .where(FlightSeat.status == "AVAILABLE")
                    .limit(1)
                )
                flight_seat_id = seat_res.scalar_one_or_none()
                assert flight_seat_id is not None

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                book_resp = await client.post(
                    "/api/v1/bookings",
                    json={
                        "customer_id": customer_id,
                        "tickets": [
                            {
                                "flight_seat_id": flight_seat_id,
                                "passenger_name": "Vault Test Passenger",
                                "price": "189.00",
                            }
                        ],
                    },
                )
                assert book_resp.status_code == 201, book_resp.text
                book_data = book_resp.json()
                assert len(book_data["tickets"]) == 1
                ticket_id = book_data["tickets"][0]["ticket_id"]
                booking_code = book_data["booking_code"]

                # 2. GET /api/v1/tickets/{ticket_id} (Cache-Aside)
                t_resp = await client.get(f"/api/v1/tickets/{ticket_id}")
                assert t_resp.status_code == 200, t_resp.text
                t_data = t_resp.json()
                assert t_data["ticket_id"] == ticket_id
                assert t_data["passenger_name"] == "Vault Test Passenger"
                assert t_data["status"] == "ACTIVE"

                # 3. GET non-existent ticket -> 404 Not Found
                not_found = await client.get("/api/v1/tickets/99999999")
                assert not_found.status_code == 404

                # 4. GET /api/v1/tickets/{ticket_id}/pdf?redirect=false (JSON download URL payload)
                pdf_json = await client.get(f"/api/v1/tickets/{ticket_id}/pdf?redirect=false")
                assert pdf_json.status_code == 200, pdf_json.text
                json_data = pdf_json.json()
                assert "download_url" in json_data
                assert json_data["ticket_id"] == ticket_id
                assert json_data["booking_code"] == booking_code

                # 5. GET /api/v1/tickets/{ticket_id}/pdf (307 Temporary Redirect)
                pdf_redirect = await client.get(
                    f"/api/v1/tickets/{ticket_id}/pdf",
                    follow_redirects=False,
                )
                assert pdf_redirect.status_code == 307
                location = pdf_redirect.headers.get("location")
                assert location is not None
                assert "X-Amz-Signature" in location

                # Download through the presigned URL
                async with httpx.AsyncClient() as external_client:
                    dl_resp = await external_client.get(location)
                    assert dl_resp.status_code == 200
                    assert dl_resp.content.startswith(b"%PDF")
    finally:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_minio_purge_keeps_bucket_intact():
    """Verify purge_all_objects cleans up all vault objects while keeping bucket intact."""
    # Upload temporary object
    dummy_pdf = b"%PDF-1.4 dummy pdf content"
    key = minio_service.upload_ticket_pdf("PURGE-TEST", 777, dummy_pdf)
    assert minio_service.object_exists(key) is True

    # Purge objects
    purged_count = minio_service.purge_all_objects()
    assert purged_count >= 1
    assert minio_service.object_exists(key) is False

    # Bucket still exists and ready
    assert minio_service.client.bucket_exists(minio_service._bucket_name) is True
