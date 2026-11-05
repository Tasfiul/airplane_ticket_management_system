import asyncio
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from app.core.config import settings
from app.core.database import async_engine, async_session_factory
from app.core.rate_limiter import SlidingWindowRateLimiter
from app.core.redis import close_redis_pool, get_redis_client
from app.routers import bookings_router, customers_router, flights_router
from app.services.stream_worker import stream_worker

logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize Redis connection and launch stream worker loop
    logger.info("Initializing Redis and Stream Consumer Worker...")
    redis = get_redis_client()
    await redis.ping()
    await stream_worker.init_consumer_group()

    # Launch Redis Stream worker as a background task
    worker_task = asyncio.create_task(stream_worker.run_loop())
    logger.info("Redis Stream DB worker task launched successfully.")

    yield

    # Shutdown: stop worker and clean up connections
    logger.info("Shutting down application...")
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

    overall_healthy = redis_status == "UP" and db_status == "UP"
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
        },
    )


# Include API v1 Routers
api_v1_prefix = "/api/v1"
app.include_router(customers_router, prefix=api_v1_prefix)
app.include_router(flights_router, prefix=api_v1_prefix)
app.include_router(bookings_router, prefix=api_v1_prefix)
