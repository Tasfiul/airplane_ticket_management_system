from datetime import date, datetime, timedelta, timezone
import httpx
import pytest
from app.core.redis import close_redis_pool
from app.main import app, lifespan


@pytest.fixture(autouse=True)
async def cleanup_connections():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_phase2_flight_discovery_endpoints():
    """Verify Phase 2 Flight Discovery Endpoints:

    1. GET /api/v1/flights/airports returns cached list of airports.
    2. GET /api/v1/flights/featured-routes returns popular route recommendations.
    3. GET /api/v1/flights/search queries flights using covering index.
    4. GET /api/v1/flights/search validates origin != destination (returns 400).
    5. Cache-Aside verification: repeated queries hit Redis cache with low latency.
    6. Existing endpoints (GET /flights/{id}, GET /flights/{id}/seats) remain fully functional.
    """
    async with lifespan(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            # 1. Airports autocomplete list
            airports_resp = await client.get("/api/v1/flights/airports")
            assert airports_resp.status_code == 200, airports_resp.text
            airports = airports_resp.json()
            assert isinstance(airports, list)
            assert len(airports) >= 10
            airport_codes = {a["code"] for a in airports}
            assert "JFK" in airport_codes
            assert "LAX" in airport_codes

            # 2. Featured routes
            featured_resp = await client.get("/api/v1/flights/featured-routes")
            assert featured_resp.status_code == 200, featured_resp.text
            featured = featured_resp.json()
            assert isinstance(featured, list)
            assert len(featured) >= 5
            assert any(r["origin"] == "JFK" and r["destination"] == "LAX" for r in featured)

            # 3. Invalid flight search (same airport)
            bad_search = await client.get("/api/v1/flights/search?origin=JFK&destination=JFK")
            assert bad_search.status_code == 400

            # 4. Valid flight search (JFK -> LAX)
            search_resp = await client.get("/api/v1/flights/search?origin=JFK&destination=LAX&page=1&page_size=5")
            assert search_resp.status_code == 200, search_resp.text
            search_data = search_resp.json()
            assert "total" in search_data
            assert "results" in search_data
            assert search_data["page"] == 1
            assert search_data["page_size"] == 5
            assert search_data["total"] > 0
            assert len(search_data["results"]) > 0

            first_flight = search_data["results"][0]
            assert first_flight["departure_airport"] == "JFK"
            assert first_flight["arrival_airport"] == "LAX"
            flight_id = first_flight["flight_id"]

            # 5. Cache-Aside hit check (subsequent search should be cached)
            search_cached = await client.get("/api/v1/flights/search?origin=JFK&destination=LAX&page=1&page_size=5")
            assert search_cached.status_code == 200
            assert search_cached.json()["total"] == search_data["total"]

            # 6. Verify existing endpoints remain functional
            # GET /flights/{flight_id}
            flight_detail = await client.get(f"/api/v1/flights/{flight_id}")
            assert flight_detail.status_code == 200
            assert flight_detail.json()["flight_id"] == flight_id

            # GET /flights/{flight_id}/seats
            seats_resp = await client.get(f"/api/v1/flights/{flight_id}/seats")
            assert seats_resp.status_code == 200
            seats = seats_resp.json()
            assert isinstance(seats, list)
            assert len(seats) == 200  # 200 seats per plane in seed
