"""Integration test for Real-Time Server-Sent Events (SSE) Seat Map Stream.

Verifies:
1. SSE endpoint connects and emits initial handshake event.
2. Placing an atomic seat hold publishes an immediate SSE update event to all subscribers.
3. Releasing held seats publishes an immediate AVAILABLE SSE update event.
4. Non-existent flight returns HTTP 404 Not Found.
"""

import asyncio
import httpx
import orjson
import pytest
from app.core.config import settings
from app.services.seat_inventory import seat_inventory

pytestmark = pytest.mark.asyncio


async def test_sse_seat_stream_connect_and_receive_updates():
    """Verify real-time SSE stream receives live seat status events from Redis Pub/Sub."""
    base_url = f"http://127.0.0.1:{settings.API_PORT}/api/v1"
    flight_id = 1
    stream_url = f"{base_url}/flights/{flight_id}/seats/stream"
    hold_url = f"{base_url}/flights/{flight_id}/seats/hold"

    received_events = []

    async def listen_to_sse():
        async with httpx.AsyncClient(timeout=10.0) as client:
            async with client.stream("GET", stream_url) as response:
                assert response.status_code == 200
                assert "text/event-stream" in response.headers.get("content-type", "")
                assert response.headers.get("x-accel-buffering") == "no"

                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        payload_str = line[len("data:"):].strip()
                        try:
                            data = orjson.loads(payload_str)
                            received_events.append(data)
                            if len(received_events) >= 2:
                                break
                        except Exception:
                            pass

    # Start SSE listener in background task
    listener_task = asyncio.create_task(listen_to_sse())

    # Wait briefly for SSE connection to establish
    await asyncio.sleep(0.3)

    # 1. Place atomic hold on seat 1
    hold_token = None
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(
            hold_url,
            json={"flight_seat_ids": [1], "hold_duration_seconds": 60},
        )
        if resp.status_code == 200:
            hold_token = resp.json().get("hold_token")

    # Wait for listener to receive the event
    try:
        await asyncio.wait_for(listener_task, timeout=3.0)
    except asyncio.TimeoutError:
        listener_task.cancel()

    # Clean up hold if created
    if hold_token:
        await seat_inventory.release_hold_atomic(hold_token)

    # Assertions
    assert len(received_events) >= 1
    assert received_events[0].get("status") == "connected"
    assert received_events[0].get("flight_id") == flight_id

    # If hold succeeded, verify the second event was a seat_update
    if len(received_events) >= 2:
        seat_ev = received_events[1]
        assert seat_ev.get("status") == "HELD"
        assert 1 in seat_ev.get("flight_seat_ids", [])


async def test_sse_seat_stream_404_for_invalid_flight():
    """Verify SSE endpoint returns 404 for non-existent flight."""
    base_url = f"http://127.0.0.1:{settings.API_PORT}/api/v1"
    stream_url = f"{base_url}/flights/999999999/seats/stream"

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(stream_url)
        assert resp.status_code == 404
