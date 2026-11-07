from datetime import datetime, timezone
import time
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.database import async_engine, get_db
from app.core.redis import get_redis_client
from app.models.plane import Plane
from app.schemas.plane import (
    AdminMetricsResponse,
    PlaneCreate,
    PlaneListResponse,
    PlaneResponse,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.minio_storage import minio_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/admin", tags=["Admin & Fleet"])


@router.post(
    "/planes",
    response_model=PlaneResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_plane",
    responses={
        201: {"model": PlaneResponse, "description": "Aircraft plane registered successfully"},
        400: {"description": "Bad Request or Plane with tail number already exists"},
    },
    summary="Register new aircraft plane & generate seat layout (via Redis Stream)",
)
async def create_plane(payload: PlaneCreate) -> PlaneResponse:
    """Register a new physical aircraft and generate its seat layout.

    Follows the strict architectural guarantee: database writes are dispatched
    strictly through the Redis Stream write pipeline (`WriteAction.CREATE_PLANE`)
    and committed in bulk by stream workers via native binary COPY protocol.
    """
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_PLANE,
        entity="plane",
        payload=payload.model_dump(mode="json"),
    )
    return PlaneResponse.model_validate(result)


@router.get(
    "/planes",
    response_model=PlaneListResponse,
    operation_id="list_planes",
    summary="List fleet aircraft planes (Cache-Aside)",
)
async def list_planes(
    page: int = Query(1, ge=1, le=2_147_483_647, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
    db: AsyncSession = Depends(get_db),
) -> PlaneListResponse:
    """Retrieve paginated aircraft planes from the fleet catalog using Cache-Aside.

    Cache misses query PostgreSQL through PgBouncer connection pooling.
    """
    cache_key = f"cache:admin:planes:p{page}:s{page_size}"

    async def fetch_planes_from_db() -> dict[str, Any]:
        count_query = select(func.count(Plane.plane_id))
        total_count = (await db.execute(count_query)).scalar() or 0

        query = (
            select(Plane)
            .order_by(Plane.plane_id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        res = await db.execute(query)
        planes_list = res.scalars().all()

        return {
            "planes": [
                {
                    "plane_id": p.plane_id,
                    "tail_number": p.tail_number,
                    "model": p.model,
                    "total_seats": p.total_seats,
                }
                for p in planes_list
            ],
            "total": total_count,
            "page": page,
            "page_size": page_size,
        }

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_planes_from_db,
        ttl=1800,
    )
    return PlaneListResponse.model_validate(cached_data)


@router.get(
    "/metrics",
    response_model=AdminMetricsResponse,
    operation_id="get_system_metrics",
    summary="System metrics: Redis Stream lag, PgBouncer pool, and MinIO health",
)
async def get_system_metrics(db: AsyncSession = Depends(get_db)) -> AdminMetricsResponse:
    """Inspect real-time operational health and metrics:

    - Redis Stream queue depth, undelivered backlog lag, and memory usage.
    - PgBouncer / PostgreSQL connection pool status and query latency.
    - MinIO document vault connectivity and bucket status.
    - Stream worker configuration parameters.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    redis = get_redis_client()

    # 1. Redis Metrics
    redis_metrics: dict[str, Any] = {}
    try:
        mem_info = await redis.info("memory")
        client_info = await redis.info("clients")
        stream_name = settings.REDIS_STREAM_WRITE_NAME
        consumer_group = settings.REDIS_STREAM_CONSUMER_GROUP
        stream_len = await redis.xlen(stream_name)

        # Consumer group info & lag
        group_lag = 0
        pending_count = 0
        try:
            groups = await redis.xinfo_groups(stream_name)
            for g in groups:
                if g.get("name") == consumer_group:
                    group_lag = int(g.get("lag", 0) or 0)
                    pending_count = int(g.get("pending", 0) or 0)
        except Exception:
            pass

        redis_metrics = {
            "status": "UP",
            "stream_name": stream_name,
            "stream_length": stream_len,
            "consumer_group_lag": group_lag,
            "pending_messages": pending_count,
            "connected_clients": client_info.get("connected_clients", 0),
            "used_memory_human": mem_info.get("used_memory_human", "N/A"),
            "used_memory_peak_human": mem_info.get("used_memory_peak_human", "N/A"),
        }
    except Exception as e:
        redis_metrics = {"status": "ERROR", "error": str(e)}

    # 2. Database / PgBouncer Metrics
    db_metrics: dict[str, Any] = {}
    try:
        t0 = time.perf_counter()
        await db.execute(text("SELECT 1;"))
        ping_ms = round((time.perf_counter() - t0) * 1000, 2)

        pool = async_engine.pool
        db_metrics = {
            "status": "UP",
            "ping_ms": ping_ms,
            "pool_size": pool.size() if hasattr(pool, "size") else "N/A",
            "checked_in_connections": pool.checkedin() if hasattr(pool, "checkedin") else "N/A",
            "checked_out_connections": pool.checkedout() if hasattr(pool, "checkedout") else "N/A",
            "overflow": pool.overflow() if hasattr(pool, "overflow") else "N/A",
        }
    except Exception as e:
        db_metrics = {"status": "ERROR", "error": str(e)}

    # 3. Stream Worker Configuration
    worker_metrics = {
        "status": "CONFIGURED",
        "consumer_group": settings.REDIS_STREAM_CONSUMER_GROUP,
        "min_batch_size": settings.STREAM_WORKER_MIN_BATCH_SIZE,
        "max_batch_size": settings.STREAM_WORKER_MAX_BATCH_SIZE,
        "queue_depth_threshold": settings.STREAM_WORKER_QUEUE_DEPTH_THRESHOLD,
        "batch_timeout_ms": settings.STREAM_WORKER_BATCH_TIMEOUT_MS,
        "block_ms": settings.STREAM_WORKER_BLOCK_MS,
    }

    # 4. MinIO Metrics
    minio_metrics: dict[str, Any] = {}
    try:
        bucket_exists = minio_service.client.bucket_exists(settings.MINIO_BUCKET_NAME)
        minio_metrics = {
            "status": "UP",
            "bucket": settings.MINIO_BUCKET_NAME,
            "bucket_exists": bucket_exists,
            "endpoint": settings.minio_endpoint,
        }
    except Exception as e:
        minio_metrics = {"status": "ERROR", "error": str(e)}

    overall_status = "healthy"
    if (
        redis_metrics.get("status") != "UP"
        or db_metrics.get("status") != "UP"
        or minio_metrics.get("status") != "UP"
    ):
        overall_status = "degraded"

    return AdminMetricsResponse(
        status=overall_status,
        timestamp=now_iso,
        redis=redis_metrics,
        database=db_metrics,
        stream_worker=worker_metrics,
        minio=minio_metrics,
    )
