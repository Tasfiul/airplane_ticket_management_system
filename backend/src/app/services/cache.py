from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any, TypeVar
import uuid
import orjson
from pydantic import BaseModel
from app.core.config import settings
from app.core.redis import get_redis_client

T = TypeVar("T")


def _orjson_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable by orjson")


class CacheService:
    """Cache-aside service powered by Redis with LRU memory eviction."""

    def __init__(self, default_ttl: int = settings.REDIS_CACHE_DEFAULT_TTL):
        self.default_ttl = default_ttl

    def _serialize(self, value: Any) -> str:
        return orjson.dumps(value, default=_orjson_default).decode("utf-8")

    def _deserialize(self, value: str | bytes) -> Any:
        return orjson.loads(value)

    async def get(self, key: str) -> Any | None:
        """Fetch an entry from Redis cache."""
        redis = get_redis_client()
        raw = await redis.get(key)
        if raw is None:
            return None
        try:
            return self._deserialize(raw)
        except Exception:
            return raw

    async def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> bool:
        """Store an entry in Redis cache with an optional TTL."""
        redis = get_redis_client()
        serialized = self._serialize(value)
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl > 0:
            await redis.set(key, serialized, ex=effective_ttl)
        else:
            await redis.set(key, serialized)
        return True

    async def delete(self, key: str) -> bool:
        """Invalidate a specific cache key."""
        redis = get_redis_client()
        deleted = await redis.delete(key)
        return deleted > 0

    async def delete_pattern(self, pattern: str, count: int = 1000) -> int:
        """Invalidate all cache keys matching a pattern using high-batch SCAN."""
        redis = get_redis_client()
        total_deleted = 0
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=count)
            if keys:
                await redis.delete(*keys)
                total_deleted += len(keys)
            if cursor == 0:
                break
        return total_deleted

    async def get_or_set(
        self,
        key: str,
        fetch_fn: Callable[[], Awaitable[T]],
        ttl: int | None = None,
    ) -> T:
        """Cache-Aside Pattern:

        1. Check cache for key.
        2. On cache hit: return cached object.
        3. On cache miss: call fetch_fn (queries DB via PgBouncer), cache the result, and return it.
        """
        cached = await self.get(key)
        if cached is not None:
            return cached

        # Cache miss: fetch from primary store (PostgreSQL via PgBouncer)
        data = await fetch_fn()
        if data is not None:
            await self.set(key, data, ttl=ttl)
        return data


cache_service = CacheService()
