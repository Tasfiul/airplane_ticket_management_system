import asyncio
from contextlib import asynccontextmanager
import logging
import time
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncpg
from sqlalchemy.exc import DBAPIError
from sqlalchemy import text
from app.core.config import settings
from app.core.database import async_engine, async_session_factory
from app.core.rate_limiter import SlidingWindowRateLimiter, rate_limiter
from app.core.redis import close_redis_pool, get_redis_client
from app.routers import (
    admin_router,
    auth_router,
    bookings_router,
    customers_router,
    flights_router,
    tickets_router,
)
from app.services.minio_storage import minio_service
from app.services.stream_worker import stream_worker

logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize Redis connection and launch stream worker loop if configured
    logger.info("Initializing Redis connection...")
    redis = get_redis_client()
    await redis.ping()

    # Initialize MinIO document vault
    logger.info("Verifying MinIO document vault...")
    minio_service.ensure_bucket_exists()

    worker_task = None
    if settings.RUN_EMBEDDED_WORKER:
        logger.info(
            "RUN_EMBEDDED_WORKER=True: Starting embedded StreamWorker inside FastAPI lifespan..."
        )
        await stream_worker.init_consumer_group()
        worker_task = asyncio.create_task(stream_worker.run_loop())
    else:
        logger.info(
            "RUN_EMBEDDED_WORKER=False: Running in decoupled mode. "
            "Dedicated stream worker container handles ingestion."
        )

    yield

    # Shutdown: stop worker if embedded, and clean up connections
    logger.info("Shutting down application...")
    if worker_task:
        stream_worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    await close_redis_pool()
    await async_engine.dispose()
    logger.info("Cleanup complete.")


app = FastAPI(
    title=settings.APP_NAME,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limiting_middleware(request: Request, call_next):
    """Enforces Redis sliding window counter rate limiting on incoming API calls."""
    if not rate_limiter.enabled:
        return await call_next(request)

    # Exclude internal healthcheck and API docs
    if request.url.path in ("/docs", "/redoc", "/openapi.json", "/health"):
        return await call_next(request)

    client_ip = (
        request.headers.get("x-forwarded-for")
        or (request.client.host if request.client else "127.0.0.1")
    ).split(",")[0].strip()

    client_key = f"{client_ip}:{request.url.path}"
    now = time.time()

    try:
        allowed, estimated, remaining, reset_seconds = await rate_limiter.check_rate_limit(
            client_key=client_key,
            now=now,
        )

        if not allowed:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Too Many Requests",
                    "message": (
                        f"Rate limit exceeded. Maximum {rate_limiter.requests_per_minute} "
                        f"requests per {rate_limiter.window_seconds} seconds."
                    ),
                    "retry_after": reset_seconds,
                },
                headers={
                    "Retry-After": str(reset_seconds),
                    "X-RateLimit-Limit": str(rate_limiter.requests_per_minute),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(now + reset_seconds)),
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(rate_limiter.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(now + reset_seconds))
        return response
    except Exception as e:
        logger.warning(f"Rate limiting degraded gracefully: {e}")
        return await call_next(request)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, str):
        detail = {"message": detail}
    return JSONResponse(
        status_code=exc.status_code,
        content=detail,
        headers=exc.headers,
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"error": "Bad Request", "message": str(exc)},
    )


@app.exception_handler(DBAPIError)
async def dbapi_error_handler(request: Request, exc: DBAPIError):
    orig = getattr(exc, "orig", None)
    if isinstance(orig, asyncpg.exceptions.UniqueViolationError):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"error": "Resource Conflict", "message": str(orig)},
        )
    if isinstance(
        orig,
        (
            asyncpg.exceptions.ForeignKeyViolationError,
            asyncpg.exceptions.CheckViolationError,
            asyncpg.exceptions.NotNullViolationError,
            asyncpg.exceptions.DataError,
        ),
    ):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Database Constraint Violation", "message": str(orig)},
        )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Database Error", "message": str(exc)},
    )


@app.exception_handler(asyncpg.exceptions.PostgresError)
async def asyncpg_postgres_error_handler(
    request: Request, exc: asyncpg.exceptions.PostgresError
):
    if isinstance(exc, asyncpg.exceptions.UniqueViolationError):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"error": "Resource Conflict", "message": str(exc)},
        )
    if isinstance(
        exc,
        (
            asyncpg.exceptions.ForeignKeyViolationError,
            asyncpg.exceptions.CheckViolationError,
            asyncpg.exceptions.NotNullViolationError,
            asyncpg.exceptions.DataError,
        ),
    ):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Database Constraint Violation", "message": str(exc)},
        )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "Database Error", "message": str(exc)},
    )


@app.get("/health", tags=["Health"])
async def health_check():
    """Healthcheck endpoint verifying PostgreSQL via PgBouncer and Redis."""
    redis_status = "DOWN"
    db_status = "DOWN"
    redis_eviction_policy = "UNKNOWN"

    try:
        redis = get_redis_client()
        pong = await redis.ping()
        if pong:
            redis_status = "UP"
            cfg = await redis.config_get("maxmemory-policy")
            redis_eviction_policy = cfg.get("maxmemory-policy", "UNKNOWN")
    except Exception as e:
        logger.error(f"Redis healthcheck failed: {e}")

    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
            db_status = "UP"
    except Exception as e:
        logger.error(f"Database healthcheck failed: {e}")

    minio_status = "DOWN"
    try:
        minio_service.ensure_bucket_exists()
        minio_status = "UP"
    except Exception as e:
        logger.error(f"MinIO healthcheck failed: {e}")

    overall_healthy = redis_status == "UP" and db_status == "UP" and minio_status == "UP"
    status_code = (
        status.HTTP_200_OK
        if overall_healthy
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )

    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if overall_healthy else "unhealthy",
            "database": {
                "status": db_status,
                "pgbouncer": settings.USE_PGBOUNCER,
            },
            "redis": {
                "status": redis_status,
                "eviction_policy": redis_eviction_policy,
            },
            "minio": {
                "status": minio_status,
                "bucket": settings.MINIO_BUCKET_NAME,
            },
        },
    )


# Include API v1 Routers
api_v1_prefix = "/api/v1"
app.include_router(auth_router, prefix=api_v1_prefix)
app.include_router(customers_router, prefix=api_v1_prefix)
app.include_router(flights_router, prefix=api_v1_prefix)
app.include_router(bookings_router, prefix=api_v1_prefix)
app.include_router(tickets_router, prefix=api_v1_prefix)
app.include_router(admin_router, prefix=api_v1_prefix)
