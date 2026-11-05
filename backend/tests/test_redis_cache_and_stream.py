import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import uuid
import httpx
import pytest
from sqlalchemy import select
from app.core.config import settings
from app.core.database import async_session_factory
from app.core.redis import close_redis_pool, get_redis_client
from app.main import app, lifespan
from app.models import Customer, Flight, FlightSeat, FlightSeatStatus, Plane, Seat, SeatClass
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.stream_worker import stream_worker
from app.services.write_stream import write_stream_service


@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_redis_server_configuration():
    """Verify Redis is running, configured with 512MB maxmemory and allkeys-lru eviction policy."""
    redis = get_redis_client()
    pong = await redis.ping()
    assert pong is True

    # Check eviction policy (less recent key eviction method = allkeys-lru)
    policy_cfg = await redis.config_get("maxmemory-policy")
    assert policy_cfg.get("maxmemory-policy") == "allkeys-lru"

    # Check maxmemory limit (512MB = 536870912 bytes)
    mem_cfg = await redis.config_get("maxmemory")
    assert int(mem_cfg.get("maxmemory", 0)) == 536870912


@pytest.mark.asyncio
async def test_cache_aside_pattern():
    """Verify cache-aside pattern:

    1. Cache miss triggers fetch_fn.
    2. Cache hit returns cached value without calling fetch_fn.
    3. Invalidation clears cache so subsequent call triggers fetch_fn again.
    """
    key = f"test:cache_aside:{uuid.uuid4().hex}"
    call_count = 0

    async def fetch_db_data():
        nonlocal call_count
        call_count += 1
        return {"data": "from_db", "count": call_count}

    # 1. First call: Cache miss
    res1 = await cache_service.get_or_set(key, fetch_db_data, ttl=60)
    assert res1["data"] == "from_db"
    assert res1["count"] == 1
    assert call_count == 1

    # 2. Second call: Cache hit (fetch_db_data is NOT called)
    res2 = await cache_service.get_or_set(key, fetch_db_data, ttl=60)
    assert res2["data"] == "from_db"
    assert res2["count"] == 1
    assert call_count == 1

    # 3. Cache deletion/invalidation
    await cache_service.delete(key)

    # 4. Third call: Cache miss again (fetch_db_data called)
    res3 = await cache_service.get_or_set(key, fetch_db_data, ttl=60)
    assert res3["data"] == "from_db"
    assert res3["count"] == 2
    assert call_count == 2


@pytest.mark.asyncio
async def test_write_through_redis_stream_via_pgbouncer():
    """Verify that DB writes are processed exclusively through Redis Stream

    and committed to PostgreSQL via PgBouncer.
    """
    # Initialize consumer group
    await stream_worker.init_consumer_group()

    test_email = f"stream_user_{uuid.uuid4().hex[:8]}@example.com"
    payload = {
        "first_name": "Stream",
        "last_name": "Writer",
        "email": test_email,
        "password": "strongpassword123",
        "city": "Sylhet",
        "postal_code": "3100",
        "division": "Sylhet",
        "country": "Bangladesh",
        "date_of_birth": "1998-04-12",
    }

    # Dispatch to Redis stream asynchronously
    event_id = await write_stream_service.dispatch_async(
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload=payload,
    )
    assert event_id is not None

    # Verify message is queued in Redis Stream
    redis = get_redis_client()
    stream_info = await redis.xlen(settings.REDIS_STREAM_WRITE_NAME)
    assert stream_info > 0

    # Before worker processes, customer should NOT exist in PostgreSQL
    async with async_session_factory() as session:
        check_before = await session.execute(
            select(Customer).where(Customer.email == test_email)
        )
        assert check_before.scalar_one_or_none() is None

    # StreamWorker processes the stream batch via PgBouncer
    processed_count = await stream_worker.process_one_batch(count=5, block=500)
    assert processed_count >= 1

    # Now verify customer exists in PostgreSQL via PgBouncer
    async with async_session_factory() as session:
        check_after = await session.execute(
            select(Customer).where(Customer.email == test_email)
        )
        saved_customer = check_after.scalar_one_or_none()
        assert saved_customer is not None
        assert saved_customer.email == test_email
        assert saved_customer.first_name == "Stream"


@pytest.mark.asyncio
async def test_http_api_write_via_redis_stream_and_read_via_cache_aside():
    """Verify complete HTTP workflow:

    - HTTP POST dispatches write via Redis Stream (never direct DB write over HTTP).
    - HTTP GET retrieves the written resource via Cache-Aside.
    """
    # Start app with its lifespan running background stream worker
    async with lifespan(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            # 1. Healthcheck
            health_resp = await client.get("/health")
            assert health_resp.status_code == 200
            health_json = health_resp.json()
            assert health_json["database"]["status"] == "UP"
            assert health_json["redis"]["status"] == "UP"
            assert health_json["redis"]["eviction_policy"] == "allkeys-lru"

            # 2. HTTP POST Customer (dispatched through Redis Stream)
            email = f"http_stream_{uuid.uuid4().hex[:8]}@example.com"
            create_payload = {
                "first_name": "Http",
                "last_name": "Streamer",
                "email": email,
                "password": "mypassword456",
                "city": "Chittagong",
                "postal_code": "4000",
                "division": "Chittagong",
                "country": "Bangladesh",
                "date_of_birth": "1992-08-15",
            }

            post_resp = await client.post("/api/v1/customers", json=create_payload)
            assert post_resp.status_code == 201
            created = post_resp.json()
            assert created["email"] == email
            customer_id = created["customer_id"]

            # 3. HTTP GET Customer by ID (served via Cache-Aside)
            get_resp = await client.get(f"/api/v1/customers/{customer_id}")
            assert get_resp.status_code == 200
            assert get_resp.json()["customer_id"] == customer_id

            # 4. Verify data is now cached in Redis
            redis = get_redis_client()
            cache_key = f"cache:customer:{customer_id}"
            cached_val = await redis.get(cache_key)
            assert cached_val is not None
            assert email in cached_val


@pytest.mark.asyncio
async def test_booking_write_pipeline_with_concurrency_lock():
    """Verify booking write operations through Redis Stream handle inventory updates

    and lock seats properly through PgBouncer.
    """
    # Setup plane, seat, flight in DB first
    async with async_session_factory() as session:
        async with session.begin():
            plane = Plane(
                tail_number=f"T-{uuid.uuid4().hex[:6].upper()}",
                model="Airbus A320",
                total_seats=1,
            )
            session.add(plane)
            await session.flush()

            seat = Seat(
                plane_id=plane.plane_id,
                seat_number="1A",
                seat_class=SeatClass.ECONOMY,
            )
            session.add(seat)
            await session.flush()

            now = datetime.now(timezone.utc)
            flight = Flight(
                flight_number=f"FL-{uuid.uuid4().hex[:4].upper()}",
                plane_id=plane.plane_id,
                departure_airport="DAC",
                arrival_airport="CXB",
                departure_time=now + timedelta(days=1),
                arrival_time=now + timedelta(days=1, hours=1),
            )
            session.add(flight)
            await session.flush()

            flight_seat = FlightSeat(
                flight_id=flight.flight_id,
                seat_id=seat.seat_id,
                status=FlightSeatStatus.AVAILABLE,
            )
            session.add(flight_seat)
            await session.flush()

            test_fs_id = flight_seat.flight_seat_id

            # Create test customer
            customer = Customer(
                first_name="Alice",
                last_name="Smith",
                email=f"alice_{uuid.uuid4().hex[:6]}@example.com",
                password_hash="dummy_hash",
                date_of_birth=date(1990, 1, 1),
            )
            session.add(customer)
            await session.flush()
            test_cust_id = customer.customer_id

    # Now book seat using write stream service with background worker running
    worker_task = asyncio.create_task(stream_worker.run_loop())

    try:
        booking_payload = {
            "customer_id": test_cust_id,
            "tickets": [
                {
                    "flight_seat_id": test_fs_id,
                    "passenger_name": "Alice Smith",
                    "price": "150.00",
                }
            ],
        }

        booking_res = await write_stream_service.dispatch_and_wait(
            action=WriteAction.CREATE_BOOKING,
            entity="booking",
            payload=booking_payload,
            timeout=5.0,
        )

        assert booking_res["booking_code"].startswith("BK-")
        assert booking_res["customer_id"] == test_cust_id
        assert len(booking_res["tickets"]) == 1

        # Verify seat is now marked BOOKED in PostgreSQL via PgBouncer
        async with async_session_factory() as session:
            seat_check = await session.get(FlightSeat, test_fs_id)
            assert seat_check.status == FlightSeatStatus.BOOKED

    finally:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_redis_lru_eviction_behavior():
    """Verify that Redis applies the configured 'allkeys-lru' (Least Recently Used)

    eviction policy when memory fills up.
    """
    redis = get_redis_client()
    orig_cfg = await redis.config_get("maxmemory")
    orig_maxmemory = int(orig_cfg.get("maxmemory", 536870912))

    try:
        await redis.flushdb()
        # Measure current baseline memory usage and set ceiling slightly above
        mem_info = await redis.info("memory")
        used_mem = int(mem_info["used_memory"])
        test_maxmemory = used_mem + 100_000
        await redis.config_set("maxmemory", str(test_maxmemory))

        # Key 1: inserted first and NOT accessed again
        await redis.set("lru_test:key1", "x" * 20000)

        # Key 2: inserted second and accessed frequently
        await redis.set("lru_test:key2", "y" * 20000)
        for _ in range(5):
            await redis.get("lru_test:key2")

        # Fill memory with new keys to force eviction
        for i in range(3, 20):
            await redis.set(f"lru_test:key{i}", "z" * 20000)
            await redis.get("lru_test:key2")  # Keep key2 fresh

        # key1 (least recently used) should have been evicted
        key1_val = await redis.get("lru_test:key1")
        assert key1_val is None

    finally:
        # Restore original maxmemory setting (512MB)
        await redis.config_set("maxmemory", str(orig_maxmemory))
        # Cleanup test keys
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match="lru_test:*", count=100)
            if keys:
                await redis.delete(*keys)
            if cursor == 0:
                break


def test_architectural_guarantee_no_direct_db_writes_over_http():
    """Verify architectural constraint:

    Routers must NEVER import direct write operations or execute session.add / session.commit.
    All write routes strictly route through write_stream_service.
    """
    import inspect
    from app.routers import bookings, customers, flights

    write_route_funcs = [
        customers.create_customer,
        flights.create_flight,
        flights.update_flight_status,
        bookings.create_booking,
        bookings.cancel_booking,
    ]

    for func in write_route_funcs:
        sig = inspect.signature(func)
        # Verify write route does NOT depend on database session
        assert "db" not in sig.parameters, f"Write route {func.__name__} must not accept db session directly!"
        source = inspect.getsource(func)
        assert "write_stream_service.dispatch" in source, (
            f"Write route {func.__name__} must dispatch write operations through write_stream_service!"
        )

