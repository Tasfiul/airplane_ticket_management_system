import uuid
import httpx
import pytest
from sqlalchemy import select, func
from app.core.config import settings
from app.core.database import async_session_factory
from app.core.redis import get_redis_client
from app.models.plane import Plane
from app.models.seat import Seat

BASE_URL = "http://localhost:8000"


@pytest.mark.asyncio
async def test_admin_create_plane_and_seats_generation():
    """Test registering an aircraft plane via Redis Stream write pipeline

    and verifying physical aircraft seat generation in PostgreSQL.
    """
    unique_suffix = uuid.uuid4().hex[:4].upper()
    tail = f"N{unique_suffix}AA"

    payload = {
        "tail_number": tail,
        "model": "Airbus A350-1000",
        "total_seats": 60,
    }

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        resp = await client.post("/api/v1/admin/planes", json=payload)
        assert resp.status_code == 201, f"Failed: {resp.text}"
        data = resp.json()

        assert data["tail_number"] == tail
        assert data["model"] == "Airbus A350-1000"
        assert data["total_seats"] == 60
        plane_id = data["plane_id"]
        assert isinstance(plane_id, int) and plane_id > 0

    # Verify directly in PostgreSQL that 60 physical seats were created
    async with async_session_factory() as session:
        seat_count_res = await session.execute(
            select(func.count(Seat.seat_id)).where(Seat.plane_id == plane_id)
        )
        assert seat_count_res.scalar() == 60

        # Verify class distribution
        seats_res = await session.execute(
            select(Seat).where(Seat.plane_id == plane_id).order_by(Seat.seat_id.asc())
        )
        seats = seats_res.scalars().all()
        assert len(seats) == 60

        first_class = [s for s in seats if s.seat_class == "FIRST"]
        biz_class = [s for s in seats if s.seat_class == "BUSINESS"]
        prem_class = [s for s in seats if s.seat_class == "PREMIUM_ECONOMY"]

        assert len(first_class) == 18
        assert len(biz_class) == 30
        assert len(prem_class) == 12


@pytest.mark.asyncio
async def test_admin_create_plane_duplicate_rejection():
    """Test duplicate tail number rejection."""
    unique_suffix = uuid.uuid4().hex[:4].upper()
    tail = f"N{unique_suffix}BB"

    payload = {
        "tail_number": tail,
        "model": "Boeing 787-10",
        "total_seats": 100,
    }

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # First creation succeeds
        resp1 = await client.post("/api/v1/admin/planes", json=payload)
        assert resp1.status_code == 201

        # Second creation with identical tail number must fail with 400
        resp2 = await client.post("/api/v1/admin/planes", json=payload)
        assert resp2.status_code == 400
        data2 = resp2.json()
        assert "already exists" in str(data2).lower() or "duplicate" in str(data2).lower()


@pytest.mark.asyncio
async def test_admin_list_planes_cache_aside():
    """Test listing planes with Cache-Aside pattern."""
    redis = get_redis_client()
    cache_key = "cache:admin:planes:p1:s15"
    await redis.delete(cache_key)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # First call: cache miss -> fetch from DB
        resp1 = await client.get("/api/v1/admin/planes?page=1&page_size=15")
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert "planes" in data1
        assert len(data1["planes"]) == 15
        assert data1["total"] >= 100
        assert data1["page"] == 1
        assert data1["page_size"] == 15

        # Verify key is populated in Redis
        cached_val = await redis.get(cache_key)
        assert cached_val is not None

        # Second call: served from cache
        resp2 = await client.get("/api/v1/admin/planes?page=1&page_size=15")
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2 == data1


@pytest.mark.asyncio
async def test_admin_system_metrics():
    """Test retrieving operational system metrics."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        resp = await client.get("/api/v1/admin/metrics")
        assert resp.status_code == 200
        data = resp.json()

        assert data["status"] in ("healthy", "UP")
        assert "timestamp" in data

        # Redis section
        assert "redis" in data
        assert data["redis"]["status"] == "UP"
        assert "stream_length" in data["redis"]
        assert "used_memory_human" in data["redis"]

        # Database section
        assert "database" in data
        assert data["database"]["status"] == "UP"
        assert isinstance(data["database"]["ping_ms"], (int, float))

        # Stream worker section
        assert "stream_worker" in data
        assert data["stream_worker"]["status"] == "CONFIGURED"
        assert data["stream_worker"]["consumer_group"] == settings.REDIS_STREAM_CONSUMER_GROUP

        # MinIO section
        assert "minio" in data
        assert data["minio"]["status"] == "UP"
        assert data["minio"]["bucket_exists"] is True
