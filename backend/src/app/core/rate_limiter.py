"""Sliding Window Counter Rate Limiter.

Implements the weighted sliding window counter algorithm using atomic Redis Hash operations.
Instead of maintaining an unbounded log of individual request timestamps (Sliding Window Log),
this algorithm approximates requests within a rolling window by weighting the previous window's
counter with the overlap fraction of the current window:

    effective_requests = (past_window_requests * (remaining_time_in_curr_window / window)) + curr_window_requests

Benefits:
  1. O(1) storage per client IP/path (fixed-size Redis Hash with two window buckets).
  2. Smooth recovery over time without hard boundary cliffs or sudden rate bursts.
  3. Single atomic Lua execution (<1ms overhead over Redis).
  4. Fully configurable via .env (RATE_LIMIT_ENABLED, RATE_LIMIT_REQUESTS_PER_MINUTE, RATE_LIMIT_WINDOW_SECONDS)
     with default fallback values and aliases.
"""

import time
from fastapi import HTTPException, Request, Response, status
from app.core.config import settings
from app.core.redis import get_redis_client

SLIDING_WINDOW_COUNTER_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local window = tonumber(ARGV[3])

-- Determine current bucket and time within the window
local current_bucket = math.floor(now / window)
local elapsed_in_window = now - (current_bucket * window)
local remaining_time_in_window = window - elapsed_in_window

-- Retrieve past window and current window counts from Redis Hash
local data = redis.call('HMGET', key, 'curr_bucket', 'curr_count', 'prev_bucket', 'prev_count')
local stored_curr_bucket = tonumber(data[1]) or 0
local stored_curr_count = tonumber(data[2]) or 0
local stored_prev_bucket = tonumber(data[3]) or 0
local stored_prev_count = tonumber(data[4]) or 0

local curr_count = 0
local prev_count = 0

if stored_curr_bucket == current_bucket then
    curr_count = stored_curr_count
    if stored_prev_bucket == (current_bucket - 1) then
        prev_count = stored_prev_count
    end
elseif stored_curr_bucket == (current_bucket - 1) then
    -- Current bucket from previous interval is now the previous bucket
    prev_count = stored_curr_count
    curr_count = 0
else
    -- Both stored buckets are older than the sliding window
    prev_count = 0
    curr_count = 0
end

-- Sliding window counter approximation:
-- effective_requests = (past_window_requests * (remaining_time_in_current_window / window)) + current_window_requests
local prev_weight = remaining_time_in_window / window
local estimated_requests = math.floor(prev_count * prev_weight) + curr_count

local reset_seconds = math.ceil(remaining_time_in_window)
if reset_seconds <= 0 then
    reset_seconds = 1
end

if estimated_requests >= limit then
    -- Rate limit exceeded: reject without incrementing
    return {0, math.floor(estimated_requests), 0, reset_seconds}
else
    -- Request permitted: increment current window bucket
    curr_count = curr_count + 1
    redis.call('HMSET', key,
        'curr_bucket', current_bucket,
        'curr_count', curr_count,
        'prev_bucket', current_bucket - 1,
        'prev_count', prev_count
    )
    redis.call('EXPIRE', key, math.ceil(window * 2))

    local new_estimated = math.floor(prev_count * prev_weight) + curr_count
    local remaining = math.max(0, limit - new_estimated)
    return {1, new_estimated, remaining, reset_seconds}
end
"""


class SlidingWindowRateLimiter:
    """Sliding window counter rate limiter powered by Redis Hashes and atomic Lua script.

    Parameters are loaded dynamically from environment / .env (via app.core.config.settings)
    with fallback defaults:
      - RATE_LIMIT_ENABLED (default: True)
      - RATE_LIMIT_REQUESTS_PER_MINUTE (default: 120)
      - RATE_LIMIT_WINDOW_SECONDS (default: 60)
    """

    def __init__(
        self,
        requests_per_minute: int | None = None,
        window_seconds: int | None = None,
        enabled: bool | None = None,
    ):
        self._requests_per_minute = requests_per_minute
        self._window_seconds = window_seconds
        self._enabled = enabled

    @property
    def requests_per_minute(self) -> int:
        return (
            self._requests_per_minute
            if self._requests_per_minute is not None
            else settings.RATE_LIMIT_REQUESTS_PER_MINUTE
        )

    @property
    def window_seconds(self) -> int:
        return (
            self._window_seconds
            if self._window_seconds is not None
            else settings.RATE_LIMIT_WINDOW_SECONDS
        )

    @property
    def enabled(self) -> bool:
        return (
            self._enabled
            if self._enabled is not None
            else settings.RATE_LIMIT_ENABLED
        )

    async def check_rate_limit(
        self,
        client_key: str,
        now: float | None = None,
    ) -> tuple[bool, int, int, int]:
        """Atomically evaluates sliding window counter via Redis Lua script.

        Returns:
          tuple: (allowed: bool, current_effective: int, remaining: int, reset_seconds: int)
        """
        if not self.enabled:
            return True, 0, self.requests_per_minute, self.window_seconds

        current_time = time.time() if now is None else float(now)
        redis = get_redis_client()
        key = f"rate_limit:{client_key}"

        res = await redis.eval(
            SLIDING_WINDOW_COUNTER_LUA,
            1,
            key,
            current_time,
            self.requests_per_minute,
            self.window_seconds,
        )
        allowed = bool(res[0])
        estimated = int(res[1])
        remaining = int(res[2])
        reset_seconds = int(res[3])
        return allowed, estimated, remaining, reset_seconds

    async def __call__(self, request: Request, response: Response) -> None:
        """FastAPI dependency callable for route rate limiting."""
        if not self.enabled:
            return

        client_ip = (
            request.headers.get("x-forwarded-for")
            or (request.client.host if request.client else "127.0.0.1")
        ).split(",")[0].strip()

        client_key = f"{client_ip}:{request.url.path}"
        now = time.time()

        try:
            allowed, estimated, remaining, reset_seconds = await self.check_rate_limit(
                client_key=client_key,
                now=now,
            )

            response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(int(now + reset_seconds))

            if not allowed:
                response.headers["Retry-After"] = str(reset_seconds)
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail={
                        "error": "Too Many Requests",
                        "message": (
                            f"Rate limit exceeded. Maximum {self.requests_per_minute} "
                            f"requests per {self.window_seconds} seconds."
                        ),
                        "retry_after": reset_seconds,
                    },
                    headers={
                        "Retry-After": str(reset_seconds),
                        "X-RateLimit-Limit": str(self.requests_per_minute),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(now + reset_seconds)),
                    },
                )
        except HTTPException:
            raise
        except Exception:
            # Degrade gracefully if Redis rate limiting encounters temporary issues
            pass


rate_limiter = SlidingWindowRateLimiter()
