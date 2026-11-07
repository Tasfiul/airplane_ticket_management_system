import asyncio
import uuid
import httpx
import pytest
from app.core.redis import close_redis_pool
from app.main import app, lifespan
from app.services.stream_worker import stream_worker


@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_phase1_auth_and_customer_endpoints():
    """Verify Phase 1 endpoints:

    1. Register customer via POST /api/v1/auth/register (sync 201 and async 202).
    2. Register customer via POST /api/v1/customers.
    3. Login via /api/v1/auth/login and receive JWT access token in JSON payload.
    4. Access /api/v1/customers/me with Bearer token (Cache-Aside).
    5. Update profile via PATCH /api/v1/customers/me (Redis Stream).
    6. Access /api/v1/customers/me/bookings (Cache-Aside eager loading).
    7. Verify 401 Unauthorized for requests without valid Bearer token.
    8. Verify 409 Conflict for duplicate registration.
    """
    await stream_worker.init_consumer_group()
    worker_task = asyncio.create_task(stream_worker.run_loop())

    try:
        async with lifespan(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                # 1. Register a new customer via POST /api/v1/auth/register
                unique_id = uuid.uuid4().hex[:8]
                email = f"auth_user_{unique_id}@example.com"
                password = "SecurePassword123!"

                reg_payload = {
                    "first_name": "Alice",
                    "last_name": "Smith",
                    "email": email,
                    "password": password,
                    "city": "Boston",
                    "postal_code": "02101",
                    "division": "MA",
                    "country": "United States",
                    "date_of_birth": "1995-04-12",
                }

                reg_resp = await client.post("/api/v1/auth/register", json=reg_payload)
                assert reg_resp.status_code == 201, reg_resp.text
                customer_data = reg_resp.json()
                customer_id = customer_data["customer_id"]
                assert customer_data["email"] == email

                # 1b. Test duplicate registration on /api/v1/auth/register -> 409 Conflict
                dup_resp = await client.post("/api/v1/auth/register", json=reg_payload)
                assert dup_resp.status_code == 409, dup_resp.text

                # 1c. Test async ingestion on /api/v1/auth/register?async_ingest=true -> 202 Accepted
                async_id = uuid.uuid4().hex[:8]
                async_payload = dict(reg_payload, email=f"async_auth_{async_id}@example.com")
                async_resp = await client.post("/api/v1/auth/register?async_ingest=true", json=async_payload)
                assert async_resp.status_code == 202, async_resp.text
                assert async_resp.json()["status"] == "PENDING"
                assert "event_id" in async_resp.json()

                # 1d. Also test POST /api/v1/customers for backward compatibility
                cust_api_id = uuid.uuid4().hex[:8]
                cust_payload = dict(reg_payload, email=f"cust_api_{cust_api_id}@example.com")
                cust_resp = await client.post("/api/v1/customers", json=cust_payload)
                assert cust_resp.status_code == 201, cust_resp.text

                # 2. Login with valid credentials
                login_payload = {"email": email, "password": password}
                login_resp = await client.post("/api/v1/auth/login", json=login_payload)
                assert login_resp.status_code == 200, login_resp.text
                token_data = login_resp.json()
                assert "access_token" in token_data
                assert token_data["token_type"] == "bearer"
                assert token_data["expires_in"] == 3600
                assert token_data["customer"]["customer_id"] == customer_id

                token = token_data["access_token"]
                auth_headers = {"Authorization": f"Bearer {token}"}

                # Login with invalid credentials should return 401
                bad_login = await client.post(
                    "/api/v1/auth/login",
                    json={"email": email, "password": "WrongPassword!"},
                )
                assert bad_login.status_code == 401

                # 3. GET /api/v1/customers/me
                me_resp = await client.get("/api/v1/customers/me", headers=auth_headers)
                assert me_resp.status_code == 200, me_resp.text
                me_data = me_resp.json()
                assert me_data["customer_id"] == customer_id
                assert me_data["first_name"] == "Alice"
                assert me_data["city"] == "Boston"

                # Unauthenticated GET /me should return 401
                unauth_resp = await client.get("/api/v1/customers/me")
                assert unauth_resp.status_code == 401

                # 4. PATCH /api/v1/customers/me (dispatched through Redis Stream)
                update_payload = {
                    "first_name": "Alicia",
                    "city": "Cambridge",
                    "division": "MA",
                }
                patch_resp = await client.patch(
                    "/api/v1/customers/me",
                    json=update_payload,
                    headers=auth_headers,
                )
                assert patch_resp.status_code == 200, patch_resp.text
                updated_data = patch_resp.json()
                assert updated_data["customer_id"] == customer_id
                assert updated_data["first_name"] == "Alicia"
                assert updated_data["city"] == "Cambridge"

                # Verify cache was invalidated and GET /me returns the updated data
                me_again = await client.get("/api/v1/customers/me", headers=auth_headers)
                assert me_again.status_code == 200
                assert me_again.json()["first_name"] == "Alicia"
                assert me_again.json()["city"] == "Cambridge"

                # 5. GET /api/v1/customers/me/bookings (should be empty list for new customer)
                bookings_resp = await client.get(
                    "/api/v1/customers/me/bookings?page=1&page_size=5",
                    headers=auth_headers,
                )
                assert bookings_resp.status_code == 200, bookings_resp.text
                assert isinstance(bookings_resp.json(), list)
                assert len(bookings_resp.json()) == 0

    finally:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
