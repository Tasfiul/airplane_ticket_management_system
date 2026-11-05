import time
import uuid
from fastapi import HTTPException, Request, Response
from app.core.redis import get_redis_client


class SlidingWindowRateLimiter:
    """Sliding window counter rate limiter powered by Redis sorted sets (ZSET)."""

    def __init__(self, requests_per_minute: int = 120, window_seconds: int = 60):
        self.requests_per_minute = requests_per_minute
        self.window_seconds = window_seconds

    async def __call__(self, request: Request, response: Response) -> None:
        client_ip = (
            request.headers.get("x-forwarded-for")
            or (request.client.host if request.client else "127.0.0.1")
        ).split(",")[0].strip()

        key = f"rate_limit:{client_ip}:{request.url.path}"
        redis = get_redis_client()

        now = time.time()
        window_start = now - self.window_seconds
        member_id = f"{now}:{uuid.uuid4().hex[:8]}"

        try:
            pipe = redis.pipeline()
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zadd(key, {member_id: now})
            pipe.zcard(key)
            pipe.expire(key, self.window_seconds)
            results = await pipe.execute()

            current_count = results[2]
            remaining = max(0, self.requests_per_minute - current_count)

            response.headers["X-RateLimit-Limit"] = str(
                self.requests_per_minute
            )
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(
                int(now + self.window_seconds)
            )

            if current_count > self.requests_per_minute:
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Too Many Requests",
                        "message": f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per {self.window_seconds} seconds.",
                        "retry_after": self.window_seconds,
                    },
                )
        except HTTPException:
            raise
        except Exception as e:
            # Degrade gracefully if Redis rate limiting encounters temporary issues
            pass
