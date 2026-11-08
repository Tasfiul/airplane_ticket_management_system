"""Unit and integration tests for Sliding Window Counter Rate Limiter using Redis."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import pytest
from fastapi import HTTPException, Request, Response
from app.core.config import Settings, settings
from app.core.rate_limiter import SlidingWindowRateLimiter, rate_limiter
from app.core.redis import get_redis_client
from app.main import app


def make_mock_request(client_ip: str = "192.168.1.100", path: str = "/api/v1/flights/search") -> Request:
    """Create a minimal mock FastAPI Request."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [(b"x-forwarded-for", client_ip.encode("utf-8"))],
        "client": ("127.0.0.1", 12345),
    }
    return Request(scope)


def test_rate_limiter_defaults_from_settings():
    """Verify rate limiter picks up settings values as defaults when not overridden."""
    limiter = SlidingWindowRateLimiter()
    assert limiter.requests_per_minute == settings.RATE_LIMIT_REQUESTS_PER_MINUTE
    assert limiter.window_seconds == settings.RATE_LIMIT_WINDOW_SECONDS
    assert limiter.enabled == settings.RATE_LIMIT_ENABLED


def test_rate_limiter_explicit_overrides():
    """Verify explicit parameters override settings defaults."""
    limiter = SlidingWindowRateLimiter(requests_per_minute=50, window_seconds=30, enabled=False)
    assert limiter.requests_per_minute == 50
    assert limiter.window_seconds == 30
    assert limiter.enabled is False


def test_rate_limiter_env_aliases():
    """Verify Settings resolves rate limit aliases from environment dict."""
    custom_settings = Settings(
        _env_file=None,
        RATE_LIMIT_RPM=300,
        RATE_LIMIT_WINDOW=120,
        ENABLE_RATE_LIMIT=False,
    )
    assert custom_settings.RATE_LIMIT_REQUESTS_PER_MINUTE == 300
    assert custom_settings.RATE_LIMIT_WINDOW_SECONDS == 120
    assert custom_settings.RATE_LIMIT_ENABLED is False


@pytest.mark.asyncio
async def test_sliding_window_counter_enforcement():
    """Test Redis sliding window counter enforcement, remaining counter, and 429 rejection."""
    redis = get_redis_client()
    test_ip = "203.0.113.199"
    test_path = "/api/v1/test/rate-limit"
    redis_key = f"rate_limit:{test_ip}"

    await redis.delete(redis_key)

    # Configure a quota: 3 requests per 60-second window
    limiter = SlidingWindowRateLimiter(requests_per_minute=3, window_seconds=60, enabled=True)
    req = make_mock_request(client_ip=test_ip, path=test_path)

    try:
        # Request 1: Allowed, remaining = 2
        resp1 = Response()
        await limiter(req, resp1)
        assert resp1.headers["X-RateLimit-Limit"] == "3"
        assert resp1.headers["X-RateLimit-Remaining"] == "2"
        assert "X-RateLimit-Reset" in resp1.headers

        # Request 2: Allowed, remaining = 1
        resp2 = Response()
        await limiter(req, resp2)
        assert resp2.headers["X-RateLimit-Remaining"] == "1"

        # Request 3: Allowed, remaining = 0
        resp3 = Response()
        await limiter(req, resp3)
        assert resp3.headers["X-RateLimit-Remaining"] == "0"

        # Request 4: Exceeded -> HTTP 429 Too Many Requests
        resp4 = Response()
        with pytest.raises(HTTPException) as exc_info:
            await limiter(req, resp4)

        assert exc_info.value.status_code == 429
        assert exc_info.value.detail["error"] == "Too Many Requests"
        assert "Maximum 3 requests per 60 seconds" in exc_info.value.detail["message"]
        assert "retry_after" in exc_info.value.detail
        assert "Retry-After" in resp4.headers
    finally:
        await redis.delete(redis_key)


@pytest.mark.asyncio
async def test_sliding_window_counter_weighted_recovery():
    """Verify the weighted recovery formula:

    effective_requests = floor(past_window_requests * (remaining_time / window)) + current_window_requests
    As time elapses into the new window, quota smoothly recovers.
    """
    redis = get_redis_client()
    client_key = "test_weighted_recovery_client"
    redis_key = f"rate_limit:{client_key}"
    await redis.delete(redis_key)

    limit = 100
    window = 60
    limiter = SlidingWindowRateLimiter(requests_per_minute=limit, window_seconds=window, enabled=True)

    try:
        # Simulate 100 requests in window bucket 0 at t=30.0s (midway through bucket 0)
        t_base = 60.0
        for _ in range(100):
            allowed, est, rem, reset = await limiter.check_rate_limit(client_key=client_key, now=t_base + 30.0)
            assert allowed is True

        # 101st request in same window is rejected
        allowed, est, rem, reset = await limiter.check_rate_limit(client_key=client_key, now=t_base + 35.0)
        assert allowed is False
        assert est == 100

        # Advance to window bucket 1 (spans [t_base + 60, t_base + 120))
        # At 25% through bucket 1 (t = t_base + 60 + 15 = t_base + 75):
        # remaining_time = 45s (weight = 45/60 = 0.75).
        # Past requests weighted = 100 * 0.75 = 75.
        # Should allow new requests and have ~24 remaining!
        allowed, est, rem, reset = await limiter.check_rate_limit(client_key=client_key, now=t_base + 75.0)
        assert allowed is True
        assert est == 76  # 75 weighted from past + 1 current
        assert rem == 24  # 100 - 76

        # At 50% through bucket 1 (t = t_base + 60 + 30 = t_base + 90):
        # remaining_time = 30s (weight = 30/60 = 0.5).
        # Past requests weighted = 100 * 0.5 = 50.
        allowed, est, rem, reset = await limiter.check_rate_limit(client_key=client_key, now=t_base + 90.0)
        assert allowed is True
        # 50 from past + 2 current requests so far = 52
        assert est == 52
        assert rem == 48

        # At 90% through bucket 1 (t = t_base + 60 + 54 = t_base + 114):
        # remaining_time = 6s (weight = 6/60 = 0.1).
        # Past requests weighted = 100 * 0.1 = 10.
        allowed, est, rem, reset = await limiter.check_rate_limit(client_key=client_key, now=t_base + 114.0)
        assert allowed is True
        # 10 from past + 3 current requests = 13
        assert est == 13
        assert rem == 87
    finally:
        await redis.delete(redis_key)


@pytest.mark.asyncio
async def test_rate_limiter_bypass_when_disabled():
    """Verify requests immediately pass through when enabled=False."""
    limiter = SlidingWindowRateLimiter(requests_per_minute=1, window_seconds=60, enabled=False)
    req = make_mock_request(client_ip="198.51.100.5", path="/api/v1/test/bypass")
    resp = Response()

    # Even 5 rapid requests should not raise 429
    for _ in range(5):
        await limiter(req, resp)


@pytest.mark.asyncio
async def test_rate_limiter_graceful_degradation_on_redis_error():
    """Verify that unexpected Redis failures do not crash API requests."""
    limiter = SlidingWindowRateLimiter(requests_per_minute=10, window_seconds=60)
    req = make_mock_request()
    resp = Response()

    with patch("app.core.rate_limiter.get_redis_client") as mock_get_redis:
        mock_redis = MagicMock()
        mock_redis.eval = AsyncMock(side_effect=RuntimeError("Redis connection lost"))
        mock_get_redis.return_value = mock_redis

        # Must not raise an exception; degrades gracefully
        await limiter(req, resp)


@pytest.mark.asyncio
async def test_endpoint_swarming_acceptance_and_rejection():
    """Swarm the live FastAPI endpoint with rapid sequential requests.

    Verifies that requests up to the rate limit quota succeed with HTTP 200,
    and all excess requests in the sliding window are rejected with HTTP 429.
    """
    redis = get_redis_client()
    swarm_ip = "198.51.100.88"
    path = "/api/v1/flights/airports"
    key = f"rate_limit:{swarm_ip}"
    await redis.delete(key)

    orig_limit = rate_limiter._requests_per_minute
    orig_window = rate_limiter._window_seconds
    rate_limiter._requests_per_minute = 5
    rate_limiter._window_seconds = 60

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            responses = []
            for _ in range(8):
                resp = await client.get(path, headers={"X-Forwarded-For": swarm_ip})
                responses.append(resp)

            # First 5 requests must succeed (200 OK)
            for i in range(5):
                assert responses[i].status_code == 200
                assert responses[i].headers["X-RateLimit-Limit"] == "5"
                assert responses[i].headers["X-RateLimit-Remaining"] == str(4 - i)

            # Remaining 3 requests must be rejected (429 Too Many Requests)
            for i in range(5, 8):
                assert responses[i].status_code == 429
                assert responses[i].headers["X-RateLimit-Remaining"] == "0"
                assert "Retry-After" in responses[i].headers
                data = responses[i].json()
                assert data["error"] == "Too Many Requests"
                assert "Maximum 5 requests per 60 seconds" in data["message"]
    finally:
        rate_limiter._requests_per_minute = orig_limit
        rate_limiter._window_seconds = orig_window
        await redis.delete(key)


@pytest.mark.asyncio
async def test_endpoint_concurrent_swarming_thread_safety():
    """Swarm the FastAPI endpoint with 20 parallel concurrent requests using asyncio.gather.

    Ensures Redis atomic Lua script prevents race conditions and strictly caps
    accepted requests at the configured threshold.
    """
    redis = get_redis_client()
    swarm_ip = "198.51.100.99"
    path = "/api/v1/flights/airports"
    key = f"rate_limit:{swarm_ip}"
    await redis.delete(key)

    orig_limit = rate_limiter._requests_per_minute
    orig_window = rate_limiter._window_seconds
    quota = 8
    rate_limiter._requests_per_minute = quota
    rate_limiter._window_seconds = 60

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # Fire 20 requests concurrently in a single burst
            tasks = [
                client.get(path, headers={"X-Forwarded-For": swarm_ip})
                for _ in range(20)
            ]
            responses = await asyncio.gather(*tasks)

            accepted = [r for r in responses if r.status_code == 200]
            rejected = [r for r in responses if r.status_code == 429]

            # Exactly `quota` (8) must be accepted, and remaining 12 must be 429
            assert len(accepted) == quota, f"Expected {quota} accepted, got {len(accepted)}"
            assert len(rejected) == 12, f"Expected 12 rejected, got {len(rejected)}"
    finally:
        rate_limiter._requests_per_minute = orig_limit
        rate_limiter._window_seconds = orig_window
        await redis.delete(key)


@pytest.mark.asyncio
async def test_global_rate_limiting_across_different_endpoints():
    """Verify global rate limiting across multiple distinct endpoint paths.

    A single client IP calling /airports, /featured-routes, and /search
    shares the same rate limit bucket.
    """
    redis = get_redis_client()
    client_ip = "198.51.100.123"
    key = f"rate_limit:{client_ip}"
    await redis.delete(key)

    orig_limit = rate_limiter._requests_per_minute
    orig_window = rate_limiter._window_seconds
    rate_limiter._requests_per_minute = 3
    rate_limiter._window_seconds = 60

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # Request 1 to /airports -> Allowed (remaining: 2)
            r1 = await client.get("/api/v1/flights/airports", headers={"X-Forwarded-For": client_ip})
            assert r1.status_code == 200
            assert r1.headers["X-RateLimit-Remaining"] == "2"

            # Request 2 to /featured-routes -> Allowed (remaining: 1)
            r2 = await client.get("/api/v1/flights/featured-routes", headers={"X-Forwarded-For": client_ip})
            assert r2.status_code == 200
            assert r2.headers["X-RateLimit-Remaining"] == "1"

            # Request 3 to /health -> Exempt from rate limiting! (remaining stays 1)
            r_health = await client.get("/health", headers={"X-Forwarded-For": client_ip})
            assert r_health.status_code == 200

            # Request 4 to /flights/search -> Allowed (remaining: 0)
            r3 = await client.get("/api/v1/flights/search?origin=JFK&destination=LAX", headers={"X-Forwarded-For": client_ip})
            assert r3.status_code == 200
            assert r3.headers["X-RateLimit-Remaining"] == "0"

            # Request 5 to /airports -> Exceeded (HTTP 429) across endpoints!
            r4 = await client.get("/api/v1/flights/airports", headers={"X-Forwarded-For": client_ip})
            assert r4.status_code == 429
            assert r4.headers["X-RateLimit-Remaining"] == "0"
            assert "Retry-After" in r4.headers
    finally:
        rate_limiter._requests_per_minute = orig_limit
        rate_limiter._window_seconds = orig_window
        await redis.delete(key)

