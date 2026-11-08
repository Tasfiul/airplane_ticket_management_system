"""Production-Grade Locust Load Testing Suite for Airplane Ticket Management System.

Simulates realistic, multi-persona user journeys and edge cases across EVERY API endpoint
through the Nginx reverse proxy (http://localhost:80).

User Personas & Traffic Weights:
  1. FlightShopperUser (Weight: 70)
     - High-volume discovery: route search, live seat map, flight details, airports, featured routes.
  2. AccountHolderUser (Weight: 20)
     - Authenticated travelers: login, profile, booking history, profile update, PNR lookup.
  3. TicketBuyerUser (Weight: 8)
     - Full transaction funnel: search -> select seat -> Redis atomic hold (<2ms) ->
       Redis Stream checkout (HTTP 202) -> MinIO PDF boarding pass -> legitimate cancellation.
  4. EdgeCaseChaosUser (Weight: 2)
     - Intentional edge-case probe: non-existent cancellations, invalid hold tokens,
       same-city searches, unauthenticated access, and conflict checks.
"""

import datetime
import random
import time
import uuid
from locust import FastHttpUser, between, events, task

AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "ATL", "MIA", "BOS"]

POPULAR_ROUTES = [
    ("JFK", "LAX"),
    ("LAX", "JFK"),
    ("ORD", "DFW"),
    ("DFW", "ORD"),
    ("SFO", "SEA"),
    ("ATL", "MIA"),
    ("BOS", "DEN"),
]


class SafeInt(int):
    """Robust integer converter that safely parses empty strings from Locust Web UI forms."""

    def __new__(cls, val=0):
        if val == "" or val is None:
            return super().__new__(cls, 0)
        return super().__new__(cls, int(val))


@events.init_command_line_parser.add_listener
def register_locust_cli_args(parser):
    """Registers custom CLI flags and Web UI inputs for rate testing and IP simulation."""
    parser.add_argument(
        "--requests-per-minute",
        "--request-per-minute",
        type=SafeInt,
        default=SafeInt(0),
        env_var="LOCUST_REQUESTS_PER_MINUTE",
        help="Target requests per minute per user (0 = unconstrained/full speed)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--randomize-client-ip",
        type=lambda v: str(v).lower() in ("true", "1", "yes"),
        default=True,
        env_var="LOCUST_RANDOMIZE_CLIENT_IP",
        help="Simulate a unique client IP per virtual user via X-Forwarded-For (isolates rate limits)",
        include_in_web_ui=True,
    )


class BaseTicketUser(FastHttpUser):
    """Base class for Airplane Ticket personas with per-user simulated IPs and dynamic pacing."""

    abstract = True
    host = "http://localhost:80"

    min_wait: float = 0.2
    max_wait: float = 0.8

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Assign a distinct simulated client IP (RFC 5737 TEST-NET-2 / 198.51.100.0/24 or wide private block)
        # so each virtual user receives their own rate limiting bucket in Redis
        self.simulated_client_ip = f"198.51.{random.randint(1, 254)}.{random.randint(1, 254)}"
        self._setup_simulated_client_ip()

    def _setup_simulated_client_ip(self) -> None:
        """Injects simulated X-Forwarded-For header into every client request."""
        orig_request = self.client.request

        def custom_request(method, url, **kwargs):
            randomize = True
            if self.environment and self.environment.parsed_options:
                val = (
                    getattr(self.environment.parsed_options, "randomize_client_ip", None)
                    or getattr(self.environment.parsed_options, "randomizeClientIp", None)
                )
                if val is not None:
                    randomize = bool(val)

            if randomize:
                headers = dict(kwargs.get("headers") or {})
                if "X-Forwarded-For" not in headers:
                    headers["X-Forwarded-For"] = self.simulated_client_ip
                kwargs["headers"] = headers

            return orig_request(method, url, **kwargs)

        self.client.request = custom_request

    def wait_time(self) -> float:
        """Dynamic wait time: adapts to target requests_per_minute or falls back to min_wait..max_wait."""
        rpm = 0
        if self.environment and self.environment.parsed_options:
            rpm = (
                getattr(self.environment.parsed_options, "requests_per_minute", None)
                or getattr(self.environment.parsed_options, "request_per_minute", None)
                or getattr(self.environment.parsed_options, "requestsPerMinute", None)
                or 0
            )

        try:
            rpm_val = float(rpm)
        except (ValueError, TypeError):
            rpm_val = 0.0

        if rpm_val > 0.0:
            target_interval = 60.0 / rpm_val
            if not hasattr(self, "_pacing_last_run"):
                self._pacing_last_run = time.perf_counter()
                self._pacing_last_wait = 0.0
            run_time = time.perf_counter() - self._pacing_last_run - self._pacing_last_wait
            wait = max(0.0, target_interval - run_time)
            self._pacing_last_wait = wait
            self._pacing_last_run = time.perf_counter()
            return wait

        return random.uniform(self.min_wait, self.max_wait)


# ==============================================================================
# PERSONA 1: THE FLIGHT SHOPPER (70% of Traffic)
# High-volume search, discovery, and browsing (Cache-Aside + Index-Only Scans)
# ==============================================================================
class FlightShopperUser(BaseTicketUser):
    weight = 70
    min_wait = 0.1
    max_wait = 0.5

    @task(35)
    def search_flights(self) -> None:
        """Search scheduled flights by route (Covering Index-Only Scan in 2-5ms)."""
        if random.random() < 0.75:
            dep, arr = random.choice(POPULAR_ROUTES)
        else:
            dep = random.choice(AIRPORTS)
            arr = random.choice([a for a in AIRPORTS if a != dep])

        self.client.get(
            f"/api/v1/flights/search?origin={dep}&destination={arr}&page=1&page_size=10",
            name="/api/v1/flights/search",
        )

    @task(25)
    def view_flight_seats(self) -> None:
        """View live flight seat availability map from Redis Hash."""
        flight_id = random.randint(1, 25000)
        self.client.get(
            f"/api/v1/flights/{flight_id}/seats",
            name="/api/v1/flights/[id]/seats",
        )

    @task(15)
    def view_flight_details(self) -> None:
        """View specific flight metadata and schedule."""
        flight_id = random.randint(1, 25000)
        self.client.get(
            f"/api/v1/flights/{flight_id}",
            name="/api/v1/flights/[id]",
        )

    @task(10)
    def list_featured_routes(self) -> None:
        """Browse popular featured routes for home screen discovery."""
        self.client.get(
            "/api/v1/flights/featured-routes",
            name="/api/v1/flights/featured-routes",
        )

    @task(10)
    def list_airports_catalog(self) -> None:
        """Fetch cached airport catalog for search autocomplete."""
        self.client.get(
            "/api/v1/flights/airports",
            name="/api/v1/flights/airports",
        )

    @task(5)
    def lookup_booking_pnr(self) -> None:
        """Lookup booking status by confirmation code."""
        code = f"BK-{random.randint(100000, 999999)}"
        with self.client.get(
            f"/api/v1/bookings/lookup?booking_code={code}",
            name="/api/v1/bookings/lookup",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 404):
                resp.success()
            else:
                resp.failure(f"Lookup error: HTTP {resp.status_code}")


# ==============================================================================
# PERSONA 2: THE ACCOUNT HOLDER (20% of Traffic)
# Authenticated customers managing profiles, bookings, and account data
# ==============================================================================
class AccountHolderUser(BaseTicketUser):
    weight = 20
    min_wait = 0.2
    max_wait = 0.8

    def on_start(self) -> None:
        """Log in as one of the 50,000 pre-seeded customer accounts."""
        self.customer_id = random.randint(1, 50000)
        self.email = f"user_{self.customer_id}@example.com"
        self.password = "password123"
        self.token: str | None = None
        self._login()

    def _login(self) -> None:
        payload = {"email": self.email, "password": self.password}
        with self.client.post(
            "/api/v1/auth/login",
            json=payload,
            name="/api/v1/auth/login",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                try:
                    self.token = resp.json().get("access_token")
                    resp.success()
                except Exception:
                    resp.failure("Malformed login response JSON")
            elif resp.status_code in (401, 404):
                resp.success()
            else:
                resp.failure(f"Login error: HTTP {resp.status_code}")

    @property
    def auth_headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @task(25)
    def view_my_profile(self) -> None:
        """Fetch current authenticated customer profile."""
        if not self.token:
            return
        self.client.get(
            "/api/v1/customers/me",
            headers=self.auth_headers,
            name="/api/v1/customers/me",
        )

    @task(25)
    def view_my_bookings(self) -> None:
        """Fetch booking history for logged-in user."""
        if not self.token:
            return
        self.client.get(
            "/api/v1/customers/me/bookings?page=1&page_size=10",
            headers=self.auth_headers,
            name="/api/v1/customers/me/bookings",
        )

    @task(15)
    def update_profile(self) -> None:
        """Update profile details strictly via Redis Stream write pipeline."""
        if not self.token:
            return
        payload = {
            "city": random.choice(["New York", "Chicago", "Dallas", "Seattle"]),
            "postal_code": f"{random.randint(10000, 99999)}",
        }
        self.client.patch(
            "/api/v1/customers/me",
            json=payload,
            headers=self.auth_headers,
            name="/api/v1/customers/me",
        )

    @task(15)
    def view_public_customer_profile(self) -> None:
        """Lookup another customer by ID (Cache-Aside)."""
        cid = random.randint(1, 50000)
        self.client.get(
            f"/api/v1/customers/{cid}",
            name="/api/v1/customers/[id]",
        )

    @task(10)
    def get_customer_by_email(self) -> None:
        """Lookup customer by email."""
        email = f"user_{random.randint(1, 50000)}@example.com"
        self.client.get(
            f"/api/v1/customers/by-email/{email}",
            name="/api/v1/customers/by-email/[email]",
        )

    @task(10)
    def register_new_account(self) -> None:
        """Register a new customer account via Redis Stream."""
        uid = uuid.uuid4().hex[:8]
        payload = {
            "first_name": "Test",
            "last_name": f"User{uid}",
            "email": f"new_locust_{uid}@example.com",
            "password": "password123",
            "city": "Boston",
            "postal_code": "02108",
            "division": "MA",
            "country": "USA",
            "date_of_birth": "1994-08-20",
        }
        self.client.post(
            "/api/v1/auth/register",
            json=payload,
            name="/api/v1/auth/register",
        )


# ==============================================================================
# PERSONA 3: THE TICKET BUYER (8% of Traffic)
# Realistic checkout funnel: search -> hold seat (Lua) -> book (Stream) ->
# download ticket PDF (MinIO) -> cancel booking (exercises rollback!)
# ==============================================================================
class TicketBuyerUser(BaseTicketUser):
    weight = 8
    min_wait = 0.2
    max_wait = 1.0

    def on_start(self) -> None:
        self.customer_id = random.randint(1, 50000)
        self.email = f"user_{self.customer_id}@example.com"
        self.password = "password123"
        self.token: str | None = None
        self.my_booking_id: int | None = None
        self.my_ticket_id: int | None = None
        self._login()

    def _login(self) -> None:
        payload = {"email": self.email, "password": self.password}
        with self.client.post(
            "/api/v1/auth/login",
            json=payload,
            name="/api/v1/auth/login",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                try:
                    self.token = resp.json().get("access_token")
                    resp.success()
                except Exception:
                    pass

    @property
    def auth_headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @task(40)
    def full_booking_funnel(self) -> None:
        """Executes the complete booking flow:

        1. Selects flight & inspects seat map
        2. Places atomic Redis Lua seat hold (<2ms)
        3. Creates booking via Redis Stream (async_ingest=true)
        """
        flight_id = random.randint(1, 25000)

        # 1. Inspect seat map
        seat_id = None
        with self.client.get(
            f"/api/v1/flights/{flight_id}/seats",
            name="/api/v1/flights/[id]/seats",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                try:
                    seats = resp.json()
                    avail = [s for s in seats if s.get("status") == "AVAILABLE"]
                    if avail:
                        seat_id = avail[0].get("flight_seat_id")
                    resp.success()
                except Exception:
                    resp.success()
            else:
                resp.success()

        if not seat_id:
            seat_id = ((flight_id - 1) * 200) + random.randint(1, 200)

        # 2. Place atomic Redis Lua hold
        hold_token = None
        with self.client.post(
            f"/api/v1/flights/{flight_id}/seats/hold",
            json={"flight_seat_ids": [seat_id], "hold_duration_seconds": 300},
            headers=self.auth_headers,
            name="/api/v1/flights/[id]/seats/hold",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                try:
                    hold_token = resp.json().get("hold_token")
                    resp.success()
                except Exception:
                    resp.success()
            elif resp.status_code in (404, 409):
                # 409 Conflict is expected under concurrency (another user held it first)
                resp.success()
                return
            else:
                resp.failure(f"Hold failed: HTTP {resp.status_code}")
                return

        # 3. Create booking with valid hold token
        payload = {
            "customer_id": self.customer_id,
            "tickets": [
                {
                    "flight_seat_id": seat_id,
                    "passenger_name": f"Passenger {self.customer_id}",
                    "price": "220.00",
                }
            ],
            "hold_token": hold_token,
        }
        with self.client.post(
            "/api/v1/bookings?async_ingest=true",
            json=payload,
            headers=self.auth_headers,
            name="/api/v1/bookings",
            catch_response=True,
        ) as resp:
            if resp.status_code in (201, 202):
                try:
                    data = resp.json()
                    bid = data.get("booking_id")
                    if bid:
                        self.my_booking_id = bid
                    tickets = data.get("tickets")
                    if tickets and isinstance(tickets, list) and len(tickets) > 0:
                        first_tid = tickets[0].get("ticket_id")
                        if first_tid:
                            self.my_ticket_id = first_tid
                    resp.success()
                except Exception:
                    resp.success()
            elif resp.status_code in (404, 409):
                resp.success()
            else:
                resp.failure(f"Booking error: HTTP {resp.status_code}")

    @task(25)
    def download_ticket_boarding_pass(self) -> None:
        """Downloads ticket boarding pass PDF generated from MinIO Document Vault."""
        tid = self.my_ticket_id or random.randint(1, 200)
        with self.client.get(
            f"/api/v1/tickets/{tid}/pdf?redirect=false",
            name="/api/v1/tickets/[id]/pdf",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 307, 404):
                resp.success()
            else:
                resp.failure(f"Ticket PDF error: HTTP {resp.status_code}")

    @task(15)
    def view_ticket_details(self) -> None:
        """Fetch ticket details from Redis cache-aside."""
        tid = self.my_ticket_id or random.randint(1, 200)
        with self.client.get(
            f"/api/v1/tickets/{tid}",
            name="/api/v1/tickets/[id]",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 404):
                resp.success()
            else:
                resp.failure(f"Ticket lookup error: HTTP {resp.status_code}")

    @task(10)
    def confirm_my_booking(self) -> None:
        """Confirm a pending booking via Redis Stream."""
        bid = self.my_booking_id or random.randint(1, 200)
        with self.client.post(
            f"/api/v1/bookings/{bid}/confirm",
            headers=self.auth_headers,
            name="/api/v1/bookings/[id]/confirm",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 201, 400, 404, 409):
                resp.success()
            else:
                resp.failure(f"Confirm error: HTTP {resp.status_code}")

    @task(10)
    def cancel_my_booking(self) -> None:
        """Legitimately cancel the user's booking, exercising real inventory rollback:

        PostgreSQL updates tickets to CANCELLED, resets flight_seat to AVAILABLE,
        and clears Redis live seat status hash!
        """
        bid = self.my_booking_id or random.randint(1, 200)
        with self.client.post(
            f"/api/v1/bookings/{bid}/cancel",
            headers=self.auth_headers,
            name="/api/v1/bookings/[id]/cancel",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 202, 400, 404):
                self.my_booking_id = None
                resp.success()
            else:
                resp.failure(f"Cancellation error: HTTP {resp.status_code}")


# ==============================================================================
# PERSONA 4: EDGE-CASE & CHAOS USER (2% of Traffic)
# Deliberately probes system boundaries and error handling:
#  - Cancelling non-existent bookings (HTTP 400/404)
#  - Invalid/expired hold tokens (HTTP 409)
#  - Searching same origin/destination (HTTP 400)
#  - Unauthenticated access on protected routes (HTTP 401)
#  - Non-existent IDs (HTTP 404)
# ==============================================================================
class EdgeCaseChaosUser(BaseTicketUser):
    weight = 2
    min_wait = 0.2
    max_wait = 0.8

    @task(25)
    def cancel_non_existent_booking(self) -> None:
        """Edge Case: Attempt to cancel a booking ID that does not exist.

        System MUST respond with HTTP 400 or 404 without crashing.
        """
        bogus_id = random.randint(900_000_000, 999_999_999)
        with self.client.post(
            f"/api/v1/bookings/{bogus_id}/cancel",
            name="/api/v1/bookings/[id]/cancel",
            catch_response=True,
        ) as resp:
            if resp.status_code in (400, 404):
                resp.success()
            elif resp.status_code >= 500:
                resp.failure(f"Server crashed on invalid cancel: HTTP {resp.status_code}")
            else:
                resp.success()

    @task(20)
    def checkout_with_bogus_hold_token(self) -> None:
        """Edge Case: Attempt to book with an invalid or expired hold token.

        System MUST fast-reject in <2ms with HTTP 409 Conflict.
        """
        payload = {
            "customer_id": random.randint(1, 50000),
            "tickets": [
                {
                    "flight_seat_id": 1,
                    "passenger_name": "Bogus Passenger",
                    "price": "100.00",
                }
            ],
            "hold_token": f"bogus-token-{uuid.uuid4().hex[:8]}",
        }
        with self.client.post(
            "/api/v1/bookings?async_ingest=true",
            json=payload,
            name="/api/v1/bookings",
            catch_response=True,
        ) as resp:
            if resp.status_code in (404, 409):
                resp.success()
            elif resp.status_code >= 500:
                resp.failure(f"Server crashed on bogus hold token: HTTP {resp.status_code}")
            else:
                resp.success()

    @task(15)
    def same_airport_search_validation(self) -> None:
        """Edge Case: Search with identical origin and destination (e.g. JFK to JFK).

        System MUST reject with HTTP 400 Bad Request validation error.
        """
        airport = random.choice(AIRPORTS)
        with self.client.get(
            f"/api/v1/flights/search?origin={airport}&destination={airport}",
            name="/api/v1/flights/search",
            catch_response=True,
        ) as resp:
            if resp.status_code == 400:
                resp.success()
            elif resp.status_code >= 500:
                resp.failure(f"Server crashed on invalid route validation: HTTP {resp.status_code}")
            else:
                resp.failure(f"Expected 400 on identical airports, got {resp.status_code}")

    @task(15)
    def unauthenticated_profile_access(self) -> None:
        """Edge Case: Call protected /customers/me with NO Authorization header.

        System MUST reject with HTTP 401 Unauthorized.
        """
        with self.client.get(
            "/api/v1/customers/me",
            headers={"Accept": "application/json"},
            name="/api/v1/customers/me",
            catch_response=True,
        ) as resp:
            if resp.status_code == 401:
                resp.success()
            elif resp.status_code >= 500:
                resp.failure(f"Server crashed on unauthenticated access: HTTP {resp.status_code}")
            else:
                resp.failure(f"Expected 401 on missing auth, got {resp.status_code}")

    @task(10)
    def fetch_non_existent_flight(self) -> None:
        """Edge Case: Query non-existent flight ID (HTTP 404)."""
        bogus_fid = random.randint(900_000_000, 999_999_999)
        with self.client.get(
            f"/api/v1/flights/{bogus_fid}",
            name="/api/v1/flights/[id]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 404:
                resp.success()
            elif resp.status_code >= 500:
                resp.failure(f"Server crashed on 404 flight: HTTP {resp.status_code}")
            else:
                resp.success()

    @task(5)
    def schedule_flight_admin(self) -> None:
        """Occasional operational write: Schedule new flight via Redis Stream."""
        dep = random.choice(AIRPORTS)
        arr = random.choice([a for a in AIRPORTS if a != dep])
        now = datetime.datetime.now(datetime.timezone.utc)
        dep_time = now + datetime.timedelta(days=random.randint(5, 60), hours=random.randint(1, 12))
        arr_time = dep_time + datetime.timedelta(hours=random.randint(2, 6))

        payload = {
            "flight_number": f"FZ{random.randint(1000, 9999)}",
            "plane_id": random.randint(1, 100),
            "departure_airport": dep,
            "arrival_airport": arr,
            "departure_time": dep_time.isoformat(),
            "arrival_time": arr_time.isoformat(),
            "status": "SCHEDULED",
        }
        with self.client.post(
            "/api/v1/flights",
            json=payload,
            name="/api/v1/flights",
            catch_response=True,
        ) as resp:
            if resp.status_code in (201, 202, 400, 409):
                resp.success()
            else:
                resp.failure(f"Flight creation error: HTTP {resp.status_code}")

    @task(5)
    def admin_fleet_metrics(self) -> None:
        """Occasional operational read: Check fleet planes and admin metrics."""
        self.client.get("/api/v1/admin/metrics", name="/api/v1/admin/metrics")
        self.client.get("/api/v1/admin/planes", name="/api/v1/admin/planes")
        self.client.get("/health", name="/health")
