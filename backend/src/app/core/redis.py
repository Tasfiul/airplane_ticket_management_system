from collections.abc import AsyncGenerator
from typing import Any
import redis.asyncio as aioredis
from redis.asyncio.connection import ConnectionPool
from app.core.config import settings


class DynamicConnectionPool(ConnectionPool):
    """Elastic, self-scaling Redis connection pool.

    Dynamically calculates max_connections using the formula:
        max_connections = min(
            server_limit,
            max(min_connections, (in_use_connections + buffer) * multiplier)
        )
    Ensures high-concurrency benchmarks and traffic bursts never hit 'Too many connections'.
    """

    def __init__(
        self,
        *args: Any,
        min_connections: int = 1000,
        multiplier: int = 3,
        server_limit: int = 10000,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self._min_connections = min_connections
        self._multiplier = multiplier
        self._server_limit = server_limit

    @property
    def max_connections(self) -> int:
        active = len(self._in_use_connections)
        dynamic_cap = max(self._min_connections, (active + 5) * self._multiplier)
        return min(dynamic_cap, self._server_limit)

    @max_connections.setter
    def max_connections(self, value: int) -> None:
        self._min_connections = max(1, value)

    def tune_for_concurrency(
        self, concurrency: int, multiplier: int | None = None
    ) -> int:
        """Explicitly set pool floor based on runtime concurrency:

        floor = max(1000, concurrency * multiplier)
        """
        mult = multiplier or self._multiplier
        self._min_connections = min(
            max(1000, concurrency * mult), self._server_limit
        )
        return self.max_connections


_redis_client: aioredis.Redis | None = None


def get_redis_client() -> aioredis.Redis:
    """Return the global async Redis client instance, initializing if needed."""
    global _redis_client
    if _redis_client is None:
        pool = DynamicConnectionPool.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            min_connections=getattr(settings, "REDIS_MAX_CONNECTIONS", 1000),
            multiplier=3,
            server_limit=10000,
        )
        _redis_client = aioredis.Redis(connection_pool=pool)
    return _redis_client


def tune_redis_connection_pool(
    concurrent_connections: int, multiplier: int = 3
) -> int:
    """Dynamically adjust the Redis connection pool capacity for runtime concurrency:

    formula: max_connections = max(1000, concurrent_connections * multiplier)
    Returns the new pool capacity.
    """
    client = get_redis_client()
    pool = client.connection_pool
    if isinstance(pool, DynamicConnectionPool):
        return pool.tune_for_concurrency(concurrent_connections, multiplier)
    else:
        new_limit = min(
            max(1000, concurrent_connections * multiplier), 10000
        )
        pool.max_connections = new_limit
        return new_limit


async def close_redis_pool() -> None:
    """Close the global Redis client connection pool."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """FastAPI dependency yielding an async Redis client."""
    client = get_redis_client()
    yield client
