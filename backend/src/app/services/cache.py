from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal
import json
from typing import Any, TypeVar
import uuid
from pydantic import BaseModel
from app.core.config import settings
from app.core.redis import get_redis_client

T = TypeVar("T")


class EnhancedJSONEncoder(json.JSONEncoder):
    """JSON encoder supporting datetime, date, Decimal, UUID, and Pydantic models."""

    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, BaseModel):
            return o.model_dump(mode="json")
        return super().default(o)


class CacheService:
    """Cache-aside service powered by Redis with LRU memory eviction."""

    def __init__(self, default_ttl: int = settings.REDIS_CACHE_DEFAULT_TTL):
        self.default_ttl = default_ttl

    def _serialize(self, value: Any) -> str:
        return json.dumps(value, cls=EnhancedJSONEncoder)

    def _deserialize(self, value: str) -> Any:
        return json.loads(value)

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

    async def delete_pattern(self, pattern: str) -> int:
        """Invalidate all cache keys matching a pattern using SCAN."""
        redis = get_redis_client()
        count = 0
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                await redis.delete(*keys)
                count += len(keys)
            if cursor == 0:
                break
        return count

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
