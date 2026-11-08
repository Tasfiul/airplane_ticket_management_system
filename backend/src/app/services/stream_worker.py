import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
import time
from typing import Any, Callable
import uuid
import asyncpg
import bcrypt
import orjson
from redis.exceptions import ResponseError
from sqlalchemy import select
from app.core.config import settings
from app.core.database import async_session_factory
from app.core.redis import get_redis_client
from app.models import (
    Booking,
    BookingStatus,
    Customer,
    Flight,
    FlightSeat,
    FlightSeatStatus,
    FlightStatus,
    Seat,
    Ticket,
    TicketStatus,
)
from app.schemas.write_event import (
    WriteAction,
    WriteEvent,
    WriteResult,
    WriteResultStatus,
)
from app.services.cache import cache_service
from app.services.minio_storage import minio_service
from app.services.seat_inventory import seat_inventory

logger = logging.getLogger("stream_worker")


class StreamWorker:
    """Redis Stream consumer worker that performs all database write operations

    through PgBouncer in transaction pooling mode.
    Follows high-performance PostgreSQL batch write mechanics:
    - Avoids individual statements inside transactions (eliminates IPC roundtrips).
    - Bulk execution patterns:
        * Insert-only: COPY ... FROM STDIN (via asyncpg native binary protocol).
        * Upserts & bulk updates: UNNEST() array parameterization.
    - Deterministic primary key sorting for all locks and batch updates to prevent deadlocks.
    - Redis Lua cache updates and pubsub notifications executed post-commit outside the DB transaction.
    """

    def __init__(
        self,
        stream_name: str = settings.REDIS_STREAM_WRITE_NAME,
        group_name: str = settings.REDIS_STREAM_CONSUMER_GROUP,
        consumer_name: str = "worker-1",
        min_batch_size: int | None = None,
        max_batch_size: int | None = None,
        queue_depth_threshold: int | None = None,
        batch_timeout_ms: int | None = None,
        block_ms: int | None = None,
        flush_strategy: str | None = None,
    ):
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.min_batch_size = (
            min_batch_size
            if min_batch_size is not None
            else settings.STREAM_WORKER_MIN_BATCH_SIZE
        )
        self.max_batch_size = (
            max_batch_size
            if max_batch_size is not None
            else settings.STREAM_WORKER_MAX_BATCH_SIZE
        )
        self.queue_depth_threshold = (
            queue_depth_threshold
            if queue_depth_threshold is not None
            else settings.STREAM_WORKER_QUEUE_DEPTH_THRESHOLD
        )
        self.batch_timeout_ms = (
            batch_timeout_ms
            if batch_timeout_ms is not None
            else settings.STREAM_WORKER_BATCH_TIMEOUT_MS
        )
        self.block_ms = (
            block_ms
            if block_ms is not None
            else settings.STREAM_WORKER_BLOCK_MS
        )
        self.flush_strategy = (
            flush_strategy
            if flush_strategy is not None
            else settings.STREAM_WORKER_FLUSH_STRATEGY
        )
        self.is_running = False
        # Decoupled architecture: internal buffer queue separating Redis ingestion from DB flushes
        self._buffer_queue: asyncio.Queue[list[tuple[str, str, str | None]] | None] = asyncio.Queue()
        self._consumer_task: asyncio.Task | None = None
        self._flusher_task: asyncio.Task | None = None

    @staticmethod
    def _map_exception_to_status_and_message(ex: Exception) -> tuple[int, str]:
        msg = str(ex)
        if isinstance(ex, asyncpg.exceptions.UniqueViolationError):
            return (409, msg)
        if isinstance(
            ex,
            (
                asyncpg.exceptions.ForeignKeyViolationError,
                asyncpg.exceptions.CheckViolationError,
                asyncpg.exceptions.NotNullViolationError,
                asyncpg.exceptions.DataError,
                ValueError,
            ),
        ):
            return (400, msg)
        return (500, msg)

    async def init_consumer_group(self) -> None:
        """Ensure stream and consumer group exist in Redis."""
        redis = get_redis_client()
        try:
            await redis.xgroup_create(
                self.stream_name,
                self.group_name,
                id="0",
                mkstream=True,
            )
            logger.info(
                f"Created consumer group '{self.group_name}' for stream '{self.stream_name}'"
            )
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(
                    f"Consumer group '{self.group_name}' already exists."
                )
            else:
                raise

    async def get_queue_depth(self) -> int:
        """Returns the current queue depth (undelivered backlog lag)

        for this consumer group in the Redis Stream.
        """
        redis = get_redis_client()
        try:
            groups = await redis.xinfo_groups(self.stream_name)
            for group in groups:
                if group.get("name") == self.group_name:
                    lag = group.get("lag")
                    if lag is not None:
                        return int(lag)
                    return int(group.get("pending", 0))
        except Exception as e:
            logger.debug(
                f"Could not determine stream lag via xinfo_groups: {e}"
            )

        try:
            return await redis.xlen(self.stream_name)
        except Exception:
            return 0

    async def _read_stream_messages(
        self, count: int, block: int
    ) -> list[tuple[str, str, str | None]]:
        """Read up to `count` messages from Redis Stream with `block` timeout in milliseconds."""
        redis = get_redis_client()
        read_block = block if (block is not None and block > 0) else None
        try:
            read_res = await redis.xreadgroup(
                self.group_name,
                self.consumer_name,
                {self.stream_name: ">"},
                count=count,
                block=read_block,
            )
        except Exception as e:
            if "NOGROUP" in str(e):
                logger.warning(
                    f"Consumer group '{self.group_name}' missing on stream '{self.stream_name}'. Re-creating..."
                )
                try:
                    await self.init_consumer_group()
                except Exception as init_err:
                    logger.error(f"Failed to re-create consumer group: {init_err}")
            else:
                logger.error(f"Error reading from stream: {e}")
            return []

        if not read_res:
            return []

        batch: list[tuple[str, str, str | None]] = []
        for stream_key, messages in read_res:
            for msg_id, raw_data in messages:
                event_id = raw_data.get("event_id", "unknown")
                data_str = raw_data.get("data")
                batch.append((msg_id, event_id, data_str))

        return batch

    def on_batch_received(
        self,
        batch: list[tuple[str, str, str | None]],
        t_received_perf: float,
        t_received_wall: datetime,
    ) -> None:
        """Hook called when messages are received from Redis Stream."""
        pass

    def on_tx_start(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_start_perf: float,
        t_tx_start_wall: datetime,
    ) -> None:
        """Hook called when PostgreSQL transaction begins."""
        pass

    def on_item_processed(
        self,
        event_id: str,
        result: WriteResult,
        t_item_start: float,
        t_item_end: float,
        msg_id: str,
    ) -> None:
        """Hook called after an individual event in the batch is processed."""
        pass

    def on_tx_complete(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_complete_perf: float,
        t_tx_complete_wall: datetime,
    ) -> None:
        """Hook called when PostgreSQL transaction commits successfully."""
        pass

    # =========================================================================
    # High-Performance Bulk Handlers (UNNEST / COPY / Sorted Locks)
    # =========================================================================

    async def _bulk_update_flight_status(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk update flight statuses:

        1. Deduplicates multiple updates to the same flight in the batch.
        2. Sorts flight IDs ascending before execution to prevent deadlocks.
        3. Executes bulk update using UNNEST($1::bigint[], $2::text[]).
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        status_by_flight: dict[int, str] = {}
        events_by_flight: dict[int, list[str]] = {}
        keys_to_delete: list[str] = []

        for it in items:
            fid = it["flight_id"]
            st = it["status"]
            status_by_flight[fid] = st
            events_by_flight.setdefault(fid, []).append(it["event_id"])

        # Sort Batch Updates by Primary Key to prevent row-lock waits and transient deadlocks
        sorted_flight_ids = sorted(status_by_flight.keys())
        sorted_statuses = [status_by_flight[fid] for fid in sorted_flight_ids]

        # Bulk execute via UNNEST with old_status comparison
        updated_rows = await apg_conn.fetch(
            """
            UPDATE flight AS f
            SET status = v.status
            FROM (
                SELECT t.flight_id, t.status, f_old.status AS old_status
                FROM UNNEST($1::bigint[], $2::text[]) AS t(flight_id, status)
                JOIN flight f_old ON f_old.flight_id = t.flight_id
            ) AS v
            WHERE f.flight_id = v.flight_id
            RETURNING f.flight_id, f.flight_number, v.old_status, f.status AS new_status, f.departure_airport, f.arrival_airport;
            """,
            sorted_flight_ids,
            sorted_statuses,
        )

        returned_map = {r["flight_id"]: r for r in updated_rows}
        for fid, ev_ids in events_by_flight.items():
            if fid in returned_map:
                row = returned_map[fid]
                res_data = {
                    "flight_id": row["flight_id"],
                    "flight_number": row["flight_number"],
                    "status": row["new_status"],
                }
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.SUCCESS,
                        data=res_data,
                        status_code=200,
                    )
                if row["old_status"] != row["new_status"]:
                    keys_to_delete.append(f"cache:flight:{fid}")
            else:
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.ERROR,
                        error=f"Flight {fid} not found",
                        status_code=400,
                    )

        if keys_to_delete:
            post_commit_actions.append(
                lambda ks=keys_to_delete: get_redis_client().delete(*ks)
            )

        # Invalidate targeted route caches post-commit only if scheduled inventory actually changed
        affected_routes = {
            (r["departure_airport"], r["arrival_airport"])
            for r in updated_rows
            if r.get("departure_airport")
            and r.get("arrival_airport")
            and r["old_status"] != r["new_status"]
            and ("SCHEDULED" in (r["old_status"], r["new_status"]))
        }
        for dep, arr in affected_routes:
            post_commit_actions.append(
                lambda d=dep, a=arr: cache_service.delete(f"cache:search:{d}:{a}")
            )
            post_commit_actions.append(
                lambda d=dep, a=arr: cache_service.delete_pattern(f"cache:search:{d}:{a}:*")
            )

        return results

    async def _bulk_create_customer(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        pw_hashes: dict[str, str],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk create customers:

        1. Validates uniqueness within batch.
        2. Bulk checks DB duplicates via email = ANY($1::text[]).
        3. Pre-fetches sequence IDs in a single query.
        4. Inserts all valid records via COPY ... FROM STDIN (native asyncpg binary protocol).
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        seen_emails: set[str] = set()
        candidates: list[dict[str, Any]] = []

        # 1. Deduplicate within the batch
        for it in items:
            eid = it["event_id"]
            payload = it["payload"]
            email = payload.get("email", "")
            if not email:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error="Email is required",
                    status_code=400,
                )
                continue

            if email in seen_emails:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Customer with email {email} already exists",
                    status_code=409,
                )
            else:
                seen_emails.add(email)
                candidates.append(it)

        if not candidates:
            return results

        # 2. Bulk check database for existing emails
        candidate_emails = [c["payload"]["email"] for c in candidates]
        existing_rows = await apg_conn.fetch(
            "SELECT email FROM customer WHERE email = ANY($1::text[]);",
            candidate_emails,
        )
        existing_emails = {r["email"] for r in existing_rows}

        valid_candidates: list[dict[str, Any]] = []
        for c in candidates:
            eid = c["event_id"]
            email = c["payload"]["email"]
            if email in existing_emails:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Customer with email {email} already exists",
                    status_code=409,
                )
            else:
                valid_candidates.append(c)

        if not valid_candidates:
            return results

        # 3. Bulk Insert: Use COPY ... FROM STDIN (via native driver binary protocol)
        num_valid = len(valid_candidates)
        seq_rows = await apg_conn.fetch(
            "SELECT nextval('customer_customer_id_seq') AS id FROM generate_series(1, $1);",
            num_valid,
        )
        now_dt = datetime.now(timezone.utc)
        customer_records = []
        for i, c in enumerate(valid_candidates):
            c_id = seq_rows[i]["id"]
            eid = c["event_id"]
            p = c["payload"]
            pw_hash = pw_hashes.get(eid, "")
            dob = p["date_of_birth"]
            if isinstance(dob, str):
                dob = date.fromisoformat(dob)

            customer_records.append((
                c_id,
                p["first_name"],
                p["last_name"],
                p["email"],
                pw_hash,
                p.get("city"),
                p.get("postal_code"),
                p.get("division"),
                p.get("country"),
                dob,
                now_dt,
            ))

        await apg_conn.copy_records_to_table(
            "customer",
            records=customer_records,
            columns=[
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "password_hash",
                "city",
                "postal_code",
                "division",
                "country",
                "date_of_birth",
                "created_at",
            ],
        )

        cust_keys_to_delete: list[str] = []
        for i, c in enumerate(valid_candidates):
            c_id = seq_rows[i]["id"]
            eid = c["event_id"]
            p = c["payload"]
            dob = p["date_of_birth"]
            dob_str = dob if isinstance(dob, str) else dob.isoformat()
            email = p["email"]

            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "customer_id": c_id,
                    "first_name": p["first_name"],
                    "last_name": p["last_name"],
                    "email": email,
                    "city": p.get("city"),
                    "postal_code": p.get("postal_code"),
                    "division": p.get("division"),
                    "country": p.get("country"),
                    "date_of_birth": dob_str,
                    "created_at": now_dt.isoformat(),
                },
                status_code=201,
            )
            cust_keys_to_delete.extend([
                f"cache:customer:{c_id}",
                f"cache:customer:email:{email}",
            ])

        if cust_keys_to_delete:
            post_commit_actions.append(
                lambda ks=cust_keys_to_delete: get_redis_client().delete(*ks)
            )

        return results

    async def _bulk_update_customer(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk update customers:

        1. Deduplicates multiple updates to the same customer in the batch.
        2. Sorts customer IDs ascending before execution to prevent deadlocks.
        3. Executes bulk update using UNNEST().
        4. Schedules post-commit cache invalidation for updated customers.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        updates_by_cid: dict[int, dict[str, Any]] = {}
        events_by_cid: dict[int, list[str]] = {}

        for it in items:
            cid = it.get("customer_id")
            if not cid:
                results[it["event_id"]] = WriteResult(
                    event_id=it["event_id"],
                    status=WriteResultStatus.ERROR,
                    error="customer_id is required",
                    status_code=400,
                )
                continue
            payload = it.get("payload", {})
            updates_by_cid[cid] = payload
            events_by_cid.setdefault(cid, []).append(it["event_id"])

        if not updates_by_cid:
            return results

        sorted_cids = sorted(updates_by_cid.keys())
        first_names = [updates_by_cid[cid].get("first_name") for cid in sorted_cids]
        last_names = [updates_by_cid[cid].get("last_name") for cid in sorted_cids]
        cities = [updates_by_cid[cid].get("city") for cid in sorted_cids]
        postal_codes = [updates_by_cid[cid].get("postal_code") for cid in sorted_cids]
        divisions = [updates_by_cid[cid].get("division") for cid in sorted_cids]
        countries = [updates_by_cid[cid].get("country") for cid in sorted_cids]

        updated_rows = await apg_conn.fetch(
            """
            UPDATE customer AS c
            SET
                first_name = COALESCE(v.first_name, c.first_name),
                last_name = COALESCE(v.last_name, c.last_name),
                city = COALESCE(v.city, c.city),
                postal_code = COALESCE(v.postal_code, c.postal_code),
                division = COALESCE(v.division, c.division),
                country = COALESCE(v.country, c.country)
            FROM (
                SELECT * FROM UNNEST(
                    $1::bigint[],
                    $2::text[],
                    $3::text[],
                    $4::text[],
                    $5::text[],
                    $6::text[],
                    $7::text[]
                ) AS t(customer_id, first_name, last_name, city, postal_code, division, country)
            ) AS v
            WHERE c.customer_id = v.customer_id
            RETURNING c.customer_id, c.first_name, c.last_name, c.email, c.city, c.postal_code, c.division, c.country, c.date_of_birth, c.created_at;
            """,
            sorted_cids,
            first_names,
            last_names,
            cities,
            postal_codes,
            divisions,
            countries,
        )

        returned_map = {r["customer_id"]: r for r in updated_rows}
        keys_to_delete: list[str] = []
        for cid, ev_ids in events_by_cid.items():
            if cid in returned_map:
                row = returned_map[cid]
                res_data = {
                    "customer_id": row["customer_id"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "email": row["email"],
                    "city": row["city"],
                    "postal_code": row["postal_code"],
                    "division": row["division"],
                    "country": row["country"],
                    "date_of_birth": row["date_of_birth"].isoformat() if row["date_of_birth"] else None,
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                }
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.SUCCESS,
                        data=res_data,
                        status_code=200,
                    )
                # Invalidate customer caches post-commit in a single batch
                c_email = row["email"]
                keys_to_delete.extend([
                    f"cache:customer:{cid}",
                    f"cache:customer:email:{c_email}",
                    f"cache:customer:auth:{c_email}",
                ])
            else:
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.ERROR,
                        error=f"Customer {cid} not found",
                        status_code=404,
                    )

        if keys_to_delete:
            post_commit_actions.append(
                lambda ks=keys_to_delete: get_redis_client().delete(*ks)
            )

        return results

    async def _bulk_create_booking(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk create bookings and purchase tickets:

        1. Detects seat contention within the batch itself.
        2. Sorts all requested flight seat IDs ascending for FOR UPDATE locking (deadlock prevention).
        3. Bulk updates flight_seat to 'BOOKED' via UNNEST.
        4. Inserts booking records using COPY ... FROM STDIN.
        5. Inserts ticket records using COPY ... FROM STDIN.
        6. Defer atomic Redis Hash reservation & cache deletes to post-commit.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        claimed_seats: set[int] = set()
        candidate_bookings: list[dict[str, Any]] = []

        # 1. Check seat collisions within batch itself
        for it in items:
            eid = it["event_id"]
            payload = it["payload"]
            ticket_items = payload.get("tickets", [])
            if not ticket_items:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error="Booking requires at least one ticket",
                    status_code=400,
                )
                continue

            seat_ids = [t["flight_seat_id"] for t in ticket_items]
            collision = any(s in claimed_seats for s in seat_ids)
            if collision:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error="Flight seat is already booked or unavailable",
                    status_code=400,
                )
            else:
                for s in seat_ids:
                    claimed_seats.add(s)
                candidate_bookings.append(it)

        if not candidate_bookings:
            return results

        # 2. Sort all requested seat IDs in ascending order for lock acquisition
        all_requested_seat_ids = sorted(list(claimed_seats))

        # 3. Lock rows in sorted primary key order to prevent deadlocks
        locked_rows = await apg_conn.fetch(
            """
            SELECT flight_seat_id, flight_id, seat_id, status
            FROM flight_seat
            WHERE flight_seat_id = ANY($1::bigint[])
            ORDER BY flight_seat_id
            FOR UPDATE;
            """,
            all_requested_seat_ids,
        )
        seats_map = {r["flight_seat_id"]: r for r in locked_rows}

        # 4. Validate availability for each candidate booking
        valid_bookings: list[dict[str, Any]] = []
        for it in candidate_bookings:
            eid = it["event_id"]
            ticket_items = it["payload"]["tickets"]
            seat_ids = [t["flight_seat_id"] for t in ticket_items]

            missing = [s for s in seat_ids if s not in seats_map]
            if missing:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Flight seats not found: {set(missing)}",
                    status_code=404,
                )
                continue

            unavailable = [
                s for s in seat_ids if seats_map[s]["status"] != "AVAILABLE"
            ]
            if unavailable:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Flight seat {unavailable[0]} is already booked or unavailable",
                    status_code=409,
                )
                continue

            valid_bookings.append(it)

        if not valid_bookings:
            return results

        # 4b. Validate customer existence
        cust_ids = sorted(list(set(b["payload"]["customer_id"] for b in valid_bookings)))
        cust_rows = await apg_conn.fetch(
            "SELECT customer_id FROM customer WHERE customer_id = ANY($1::int[]);",
            cust_ids,
        )
        existing_custs = {r["customer_id"] for r in cust_rows}
        remaining_bookings: list[dict[str, Any]] = []
        for b in valid_bookings:
            cid = b["payload"]["customer_id"]
            if cid not in existing_custs:
                results[b["event_id"]] = WriteResult(
                    event_id=b["event_id"],
                    status=WriteResultStatus.ERROR,
                    error=f"Customer {cid} not found",
                    status_code=404,
                )
            else:
                remaining_bookings.append(b)
        valid_bookings = remaining_bookings

        if not valid_bookings:
            return results

        # 5. Bulk update flight_seat to 'BOOKED' via UNNEST (sorted by ID)
        all_booked_seat_ids = sorted([
            t["flight_seat_id"]
            for b in valid_bookings
            for t in b["payload"]["tickets"]
        ])
        await apg_conn.execute(
            """
            UPDATE flight_seat AS fs
            SET status = 'BOOKED'
            FROM (
                SELECT * FROM UNNEST($1::bigint[]) AS t(flight_seat_id)
            ) AS v
            WHERE fs.flight_seat_id = v.flight_seat_id;
            """,
            all_booked_seat_ids,
        )

        # 6. Bulk Insert Bookings using native driver binary protocol COPY
        num_bookings = len(valid_bookings)
        b_seq = await apg_conn.fetch(
            "SELECT nextval('booking_booking_id_seq') AS id FROM generate_series(1, $1);",
            num_bookings,
        )
        now_dt = datetime.now(timezone.utc)
        booking_records = []

        for i, b in enumerate(valid_bookings):
            b_id = b_seq[i]["id"]
            payload = b["payload"]
            cid = payload["customer_id"]
            for item in payload["tickets"]:
                item["price"] = Decimal(str(item["price"])).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            total_amt = sum(item["price"] for item in payload["tickets"])
            code = f"BK-{uuid.uuid4().hex[:8].upper()}"
            b["booking_code"] = code
            b["total_amount"] = total_amt
            b["booking_id"] = b_id
            b_status = payload.get("status") or "PAID"
            b["status"] = b_status

            booking_records.append((
                b_id,
                code,
                cid,
                total_amt,
                b_status,
                None,
                now_dt,
            ))

        await apg_conn.copy_records_to_table(
            "booking",
            records=booking_records,
            columns=[
                "booking_id",
                "booking_code",
                "customer_id",
                "total_amount",
                "status",
                "receipt_url",
                "created_at",
            ],
        )

        # 7. Bulk Insert Tickets using native driver binary protocol COPY
        total_tickets = sum(len(b["payload"]["tickets"]) for b in valid_bookings)
        t_seq = await apg_conn.fetch(
            "SELECT nextval('ticket_ticket_id_seq') AS id FROM generate_series(1, $1);",
            total_tickets,
        )

        ticket_records = []
        tickets_by_booking: dict[int, list[dict[str, Any]]] = {}
        t_idx = 0

        for b in valid_bookings:
            b_id = b["booking_id"]
            tickets_by_booking[b_id] = []
            for item in b["payload"]["tickets"]:
                t_id = t_seq[t_idx]["id"]
                t_idx += 1
                fs_id = item["flight_seat_id"]
                p_name = item["passenger_name"]
                price = item["price"]

                ticket_records.append((
                    t_id,
                    b_id,
                    fs_id,
                    p_name,
                    price,
                    "ACTIVE",
                    None,
                    now_dt,
                ))

                tickets_by_booking[b_id].append({
                    "ticket_id": t_id,
                    "booking_id": b_id,
                    "flight_seat_id": fs_id,
                    "passenger_name": p_name,
                    "price": str(price),
                    "status": "ACTIVE",
                    "issued_at": now_dt.isoformat(),
                })

        await apg_conn.copy_records_to_table(
            "ticket",
            records=ticket_records,
            columns=[
                "ticket_id",
                "booking_id",
                "flight_seat_id",
                "passenger_name",
                "price",
                "status",
                "ticket_url",
                "issued_at",
            ],
        )

        # 8. Post-commit Redis operations
        flight_seats_map: dict[int, list[int]] = {}
        for s_id in all_booked_seat_ids:
            row = seats_map.get(s_id)
            if row:
                fid = row["flight_id"]
                flight_seats_map.setdefault(fid, []).append(s_id)

        for fid, f_seat_ids in flight_seats_map.items():
            post_commit_actions.append(
                lambda f=fid, sids=f_seat_ids: seat_inventory.reserve_seats_atomic(f, sids)
            )
            post_commit_actions.append(
                lambda f=fid: cache_service.delete(f"cache:flight:{f}")
            )

        # 9. Format response results & post-commit cache/hold cleanups
        for b in valid_bookings:
            eid = b["event_id"]
            b_id = b["booking_id"]
            cid = b["payload"]["customer_id"]
            hold_token = b["payload"].get("hold_token")
            if hold_token:
                post_commit_actions.append(
                    lambda tok=hold_token: seat_inventory.confirm_held_seats_atomic(tok)
                )
            post_commit_actions.append(
                lambda c_id=cid: cache_service.delete_pattern(f"cache:customer:{c_id}:bookings:*")
            )
            t_items = tickets_by_booking.get(b_id, [])
            if (b.get("status") or "PAID") == "PAID" and t_items:
                b_code = b["booking_code"]
                post_commit_actions.append(
                    lambda code=b_code, items=list(t_items): self._schedule_ticket_pdf_uploads(code, items)
                )
            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "booking_id": b_id,
                    "booking_code": b["booking_code"],
                    "customer_id": cid,
                    "total_amount": str(b["total_amount"]),
                    "status": b.get("status") or "PAID",
                    "created_at": now_dt.isoformat(),
                    "tickets": tickets_by_booking.get(b_id, []),
                },
                status_code=201,
            )

        return results

    async def _bulk_cancel_booking(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk cancel bookings and release inventory:

        1. Sorts booking IDs ascending for locking.
        2. Bulk updates bookings to 'CANCELLED' via UNNEST.
        3. Bulk updates tickets to 'CANCELLED' via UNNEST.
        4. Bulk updates flight_seat rows back to 'AVAILABLE' via UNNEST (sorted by ID).
        5. Defers atomic Redis Hash release and cache deletes to post-commit.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        booking_ids = [it["booking_id"] for it in items]
        sorted_booking_ids = sorted(list(set(booking_ids)))

        # Lock bookings in sorted primary key order
        locked_bookings = await apg_conn.fetch(
            """
            SELECT booking_id, status
            FROM booking
            WHERE booking_id = ANY($1::bigint[])
            ORDER BY booking_id
            FOR UPDATE;
            """,
            sorted_booking_ids,
        )
        found_bookings = {r["booking_id"]: r for r in locked_bookings}

        valid_items = []
        for it in items:
            eid = it["event_id"]
            bid = it["booking_id"]
            if bid not in found_bookings:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Booking {bid} not found",
                    status_code=400,
                )
            else:
                valid_items.append(it)

        if not valid_items:
            return results

        valid_bids = sorted(list(set(it["booking_id"] for it in valid_items)))

        # 1. Bulk update bookings to CANCELLED via UNNEST
        await apg_conn.execute(
            """
            UPDATE booking AS b
            SET status = 'CANCELLED'
            FROM (
                SELECT * FROM UNNEST($1::bigint[]) AS t(booking_id)
            ) AS v
            WHERE b.booking_id = v.booking_id;
            """,
            valid_bids,
        )

        # 2. Query tickets for these bookings
        ticket_rows = await apg_conn.fetch(
            """
            SELECT ticket_id, booking_id, flight_seat_id, status
            FROM ticket
            WHERE booking_id = ANY($1::bigint[])
            ORDER BY ticket_id;
            """,
            valid_bids,
        )

        tickets_by_booking: dict[int, list[Any]] = {}
        for tr in ticket_rows:
            tickets_by_booking.setdefault(tr["booking_id"], []).append(tr)

        active_ticket_ids = sorted(
            [r["ticket_id"] for r in ticket_rows if r["status"] != "CANCELLED"]
        )
        if active_ticket_ids:
            await apg_conn.execute(
                """
                UPDATE ticket AS t
                SET status = 'CANCELLED'
                FROM (
                    SELECT * FROM UNNEST($1::bigint[]) AS u(ticket_id)
                ) AS v
                WHERE t.ticket_id = v.ticket_id;
                """,
                active_ticket_ids,
            )

        # 3. Bulk update flight_seat back to AVAILABLE via UNNEST (sorted by ID)
        all_seat_ids = sorted(list(set(r["flight_seat_id"] for r in ticket_rows)))
        if all_seat_ids:
            await apg_conn.execute(
                """
                UPDATE flight_seat AS fs
                SET status = 'AVAILABLE'
                FROM (
                    SELECT * FROM UNNEST($1::bigint[]) AS u(flight_seat_id)
                ) AS v
                WHERE fs.flight_seat_id = v.flight_seat_id;
                """,
                all_seat_ids,
            )

            # Query flights for Redis seat release
            flight_res = await apg_conn.fetch(
                """
                SELECT DISTINCT flight_id, flight_seat_id
                FROM flight_seat
                WHERE flight_seat_id = ANY($1::bigint[]);
                """,
                all_seat_ids,
            )
            flight_seats_map: dict[int, list[int]] = {}
            for fr in flight_res:
                flight_seats_map.setdefault(fr["flight_id"], []).append(
                    fr["flight_seat_id"]
                )

            for fid, sids in flight_seats_map.items():
                post_commit_actions.append(
                    lambda f=fid, s=sids: seat_inventory.release_seats_atomic(f, s)
                )

        for bid in valid_bids:
            post_commit_actions.append(
                lambda b=bid: cache_service.delete(f"cache:booking:{b}")
            )

        for it in valid_items:
            eid = it["event_id"]
            bid = it["booking_id"]
            t_list = tickets_by_booking.get(bid, [])
            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "booking_id": bid,
                    "status": "CANCELLED",
                    "cancelled_tickets_count": len(t_list),
                },
                status_code=200,
            )

        return results

    async def _bulk_confirm_booking(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk confirm pending bookings and transition them to PAID:

        1. Sorts all requested booking IDs ascending for row-level locking (deadlock prevention).
        2. Validates booking exists and is not CANCELLED or REFUNDED.
        3. Bulk updates booking.status to 'PAID' via UNNEST.
        4. Bulk updates ticket.status to 'ACTIVE' via UNNEST.
        5. Queries tickets for response payloads.
        6. Invalidate booking & customer cache keys and confirm held seats in Redis post-commit.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        booking_ids: list[int] = []
        for it in items:
            bid = it.get("booking_id") or it.get("payload", {}).get("booking_id")
            if not bid:
                results[it["event_id"]] = WriteResult(
                    event_id=it["event_id"],
                    status=WriteResultStatus.ERROR,
                    error="booking_id is required",
                    status_code=400,
                )
            else:
                booking_ids.append(int(bid))

        if not booking_ids:
            return results

        sorted_bids = sorted(list(set(booking_ids)))

        # 1. Lock bookings in sorted primary key order
        locked_rows = await apg_conn.fetch(
            """
            SELECT booking_id, booking_code, customer_id, total_amount, status, receipt_url, created_at
            FROM booking
            WHERE booking_id = ANY($1::bigint[])
            ORDER BY booking_id
            FOR UPDATE;
            """,
            sorted_bids,
        )
        bookings_map = {r["booking_id"]: r for r in locked_rows}

        valid_items: list[dict[str, Any]] = []
        bids_to_update: list[int] = []

        for it in items:
            eid = it["event_id"]
            bid = it.get("booking_id") or it.get("payload", {}).get("booking_id")
            if not bid or int(bid) not in bookings_map:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Booking {bid} not found",
                    status_code=404,
                )
                continue

            b_row = bookings_map[int(bid)]
            current_status = b_row["status"]

            if current_status in ("CANCELLED", "REFUNDED"):
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Booking {bid} cannot be confirmed because it is already {current_status}",
                    status_code=400,
                )
                continue

            valid_items.append(it)
            if current_status == "PENDING":
                bids_to_update.append(int(bid))

        if not valid_items:
            return results

        # 2. Bulk update PENDING bookings to PAID via UNNEST
        if bids_to_update:
            sorted_pending = sorted(list(set(bids_to_update)))
            await apg_conn.execute(
                """
                UPDATE booking AS b
                SET status = 'PAID'
                FROM (
                    SELECT * FROM UNNEST($1::bigint[]) AS t(booking_id)
                ) AS v
                WHERE b.booking_id = v.booking_id;
                """,
                sorted_pending,
            )

            # 3. Bulk update tickets of these bookings to ACTIVE via UNNEST
            await apg_conn.execute(
                """
                UPDATE ticket AS t
                SET status = 'ACTIVE'
                FROM (
                    SELECT * FROM UNNEST($1::bigint[]) AS t(booking_id)
                ) AS v
                WHERE t.booking_id = v.booking_id AND t.status != 'CANCELLED';
                """,
                sorted_pending,
            )

        # 4. Fetch all tickets for all valid bookings
        all_valid_bids = sorted(list(set(
            int(it.get("booking_id") or it.get("payload", {}).get("booking_id"))
            for it in valid_items
        )))
        ticket_rows = await apg_conn.fetch(
            """
            SELECT ticket_id, booking_id, flight_seat_id, passenger_name, price, status, ticket_url, issued_at
            FROM ticket
            WHERE booking_id = ANY($1::bigint[])
            ORDER BY ticket_id;
            """,
            all_valid_bids,
        )

        tickets_by_booking: dict[int, list[dict[str, Any]]] = {}
        for tr in ticket_rows:
            tickets_by_booking.setdefault(tr["booking_id"], []).append({
                "ticket_id": tr["ticket_id"],
                "booking_id": tr["booking_id"],
                "flight_seat_id": tr["flight_seat_id"],
                "passenger_name": tr["passenger_name"],
                "price": str(tr["price"]),
                "status": tr["status"],
                "ticket_url": tr["ticket_url"],
                "issued_at": tr["issued_at"].isoformat() if tr["issued_at"] else None,
            })

        # 5. Build cache invalidation and post-commit hooks
        keys_to_delete: list[str] = []
        for bid in all_valid_bids:
            b_row = bookings_map[bid]
            cid = b_row["customer_id"]
            keys_to_delete.append(f"cache:booking:{bid}")
            keys_to_delete.append(f"cache:booking:lookup:{b_row['booking_code']}*")
            post_commit_actions.append(
                lambda c_id=cid: cache_service.delete_pattern(f"cache:customer:{c_id}:bookings:*")
            )

        if keys_to_delete:
            post_commit_actions.append(
                lambda ks=keys_to_delete: get_redis_client().delete(*ks)
            )

        # Confirm hold tokens in Redis if provided
        for it in valid_items:
            hold_token = it.get("payload", {}).get("hold_token")
            if hold_token:
                post_commit_actions.append(
                    lambda tok=hold_token: seat_inventory.confirm_held_seats_atomic(tok)
                )

        # 6. Build response results & schedule post-commit PDF generation
        for it in valid_items:
            eid = it["event_id"]
            bid = int(it.get("booking_id") or it.get("payload", {}).get("booking_id"))
            b_row = bookings_map[bid]
            t_items = tickets_by_booking.get(bid, [])
            if t_items:
                b_code = b_row["booking_code"]
                post_commit_actions.append(
                    lambda code=b_code, items=list(t_items): self._schedule_ticket_pdf_uploads(code, items)
                )
            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "booking_id": bid,
                    "booking_code": b_row["booking_code"],
                    "customer_id": b_row["customer_id"],
                    "total_amount": str(b_row["total_amount"]),
                    "status": "PAID",
                    "receipt_url": b_row["receipt_url"],
                    "created_at": b_row["created_at"].isoformat() if b_row["created_at"] else None,
                    "tickets": t_items,
                },
                status_code=201,
            )

        return results

    def _schedule_ticket_pdf_uploads(self, booking_code: str, tickets: list[dict[str, Any]]) -> None:
        """Schedules background in-memory rendering and MinIO upload for purchased tickets.

        Runs completely decoupled from DB transaction via asyncio.create_task and to_thread.
        """
        for t in tickets:
            tid = t.get("ticket_id")
            if tid:
                asyncio.create_task(
                    asyncio.to_thread(self._render_and_upload_ticket_pdf_sync, booking_code, tid, t)
                )

    def _render_and_upload_ticket_pdf_sync(
        self, booking_code: str, ticket_id: int, ticket_data: dict[str, Any]
    ) -> None:
        """Renders boarding pass PDF in memory and uploads to MinIO vault."""
        try:
            pdf_bytes = minio_service.generate_boarding_pass_pdf(ticket_data)
            minio_service.upload_ticket_pdf(booking_code, ticket_id, pdf_bytes)
        except Exception as e:
            logger.warning(f"Background MinIO PDF upload failed for ticket {ticket_id}: {e}")

    async def _bulk_create_flight(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk create flights using fully vectorized operations:

        1. Prefetches all flight IDs via sequence generate_series in a single roundtrip.
        2. Bulk inserts all flight records using native driver binary protocol COPY ... FROM STDIN.
        3. Queries physical plane seat layouts for all distinct plane IDs in a single roundtrip.
        4. Bulk populates flight_seat inventory across all flights in ONE binary COPY stream.
        5. Registers post-commit cache invalidation once per batch.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        now_dt = datetime.now(timezone.utc)
        num_flights = len(items)

        # 1. Validate plane existence before inserting flights
        plane_ids_set: set[int] = set()
        for it in items:
            try:
                pid = int(it["payload"]["plane_id"])
                plane_ids_set.add(pid)
            except (ValueError, KeyError, TypeError):
                pass

        existing_plane_ids: set[int] = set()
        if plane_ids_set:
            plane_rows = await apg_conn.fetch(
                "SELECT plane_id FROM plane WHERE plane_id = ANY($1::int[]);",
                sorted(list(plane_ids_set)),
            )
            existing_plane_ids = {r["plane_id"] for r in plane_rows}

        valid_items: list[dict[str, Any]] = []
        for it in items:
            eid = it["event_id"]
            p = it["payload"]
            try:
                pid = int(p["plane_id"])
            except (ValueError, KeyError, TypeError):
                pid = -1
            if pid not in existing_plane_ids:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Plane {pid} not found",
                    status_code=404,
                )
            else:
                valid_items.append(it)

        if not valid_items:
            return results

        # 2. Prefetch flight IDs for valid items
        num_flights = len(valid_items)
        f_seq = await apg_conn.fetch(
            "SELECT nextval('flight_flight_id_seq') AS id FROM generate_series(1, $1);",
            num_flights,
        )

        # 3. Prepare and bulk insert flights via COPY ... FROM STDIN
        flight_records = []
        for i, it in enumerate(valid_items):
            fid = f_seq[i]["id"]
            p = it["payload"]
            dep_time = (
                datetime.fromisoformat(p["departure_time"])
                if isinstance(p["departure_time"], str)
                else p["departure_time"]
            )
            arr_time = (
                datetime.fromisoformat(p["arrival_time"])
                if isinstance(p["arrival_time"], str)
                else p["arrival_time"]
            )
            status_val = p.get("status", FlightStatus.SCHEDULED)
            if hasattr(status_val, "value"):
                status_val = status_val.value
            else:
                status_val = str(status_val)

            plane_id = int(p["plane_id"])

            flight_records.append((
                fid,
                p["flight_number"],
                plane_id,
                p["departure_airport"],
                p["arrival_airport"],
                dep_time,
                arr_time,
                status_val,
                now_dt,
            ))

        await apg_conn.copy_records_to_table(
            "flight",
            records=flight_records,
            columns=[
                "flight_id",
                "flight_number",
                "plane_id",
                "departure_airport",
                "arrival_airport",
                "departure_time",
                "arrival_time",
                "status",
                "created_at",
            ],
        )

        # 3. Query all seat IDs for the planes in a single roundtrip
        distinct_plane_ids = sorted(list(plane_ids_set))
        seat_rows = await apg_conn.fetch(
            """
            SELECT plane_id, seat_id
            FROM seat
            WHERE plane_id = ANY($1::int[])
            ORDER BY plane_id, seat_id;
            """,
            distinct_plane_ids,
        )
        seats_by_plane: dict[int, list[int]] = {}
        for r in seat_rows:
            seats_by_plane.setdefault(r["plane_id"], []).append(r["seat_id"])

        # 4. Bulk populate flight_seat inventory for all flights in ONE binary COPY stream
        all_seat_records = []
        for i, it in enumerate(valid_items):
            fid = f_seq[i]["id"]
            plane_id = int(it["payload"]["plane_id"])
            seat_ids = seats_by_plane.get(plane_id, [])
            for s_id in seat_ids:
                all_seat_records.append((
                    fid,
                    s_id,
                    FlightSeatStatus.AVAILABLE.value,
                ))

        if all_seat_records:
            await apg_conn.copy_records_to_table(
                "flight_seat",
                records=all_seat_records,
                columns=["flight_id", "seat_id", "status"],
            )

        # 5. Invalidate targeted route caches post-commit if flight is scheduled
        affected_routes = {
            (it["payload"]["departure_airport"], it["payload"]["arrival_airport"])
            for it in valid_items
            if it.get("payload")
            and "departure_airport" in it["payload"]
            and "arrival_airport" in it["payload"]
            and it["payload"].get("status", "SCHEDULED") == "SCHEDULED"
        }
        for dep, arr in affected_routes:
            post_commit_actions.append(
                lambda d=dep, a=arr: cache_service.delete(f"cache:search:{d}:{a}")
            )
            post_commit_actions.append(
                lambda d=dep, a=arr: cache_service.delete_pattern(f"cache:search:{d}:{a}:*")
            )

        # 6. Build response results
        for i, it in enumerate(valid_items):
            eid = it["event_id"]
            fid = f_seq[i]["id"]
            p = it["payload"]
            dep_time = (
                datetime.fromisoformat(p["departure_time"])
                if isinstance(p["departure_time"], str)
                else p["departure_time"]
            )
            arr_time = (
                datetime.fromisoformat(p["arrival_time"])
                if isinstance(p["arrival_time"], str)
                else p["arrival_time"]
            )
            status_val = p.get("status", FlightStatus.SCHEDULED)
            if hasattr(status_val, "value"):
                status_val = status_val.value
            else:
                status_val = str(status_val)

            plane_id = int(p["plane_id"])
            seats_count = len(seats_by_plane.get(plane_id, []))

            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "flight_id": fid,
                    "flight_number": p["flight_number"],
                    "plane_id": plane_id,
                    "departure_airport": p["departure_airport"],
                    "arrival_airport": p["arrival_airport"],
                    "departure_time": dep_time.isoformat(),
                    "arrival_time": arr_time.isoformat(),
                    "status": status_val,
                    "seats_count": seats_count,
                    "created_at": now_dt.isoformat(),
                },
                status_code=201,
            )

        return results

    async def _bulk_create_plane(
        self,
        apg_conn: asyncpg.Connection,
        items: list[dict[str, Any]],
        post_commit_actions: list[Callable[[], Any]],
    ) -> dict[str, WriteResult]:
        """Bulk create aircraft planes and physical seat layouts:
        1. Deduplicates within batch by tail_number.
        2. Bulk checks DB for existing tail_number via tail_number = ANY($1::text[]).
        3. Prefetches plane IDs via nextval('plane_plane_id_seq').
        4. Inserts planes via COPY ... FROM STDIN into table 'plane'.
        5. Prefetches seat IDs via nextval('seat_seat_id_seq') for total required seats.
        6. Inserts physical seats via COPY ... FROM STDIN into table 'seat'.
        7. Schedules cache invalidation for planes catalog post-commit.
        """
        if not items:
            return {}

        results: dict[str, WriteResult] = {}
        seen_tails: set[str] = set()
        candidates: list[dict[str, Any]] = []

        # 1. Deduplicate within batch
        for it in items:
            eid = it["event_id"]
            p = it["payload"]
            tail = (p.get("tail_number") or "").strip().upper()
            model = (p.get("model") or "").strip()
            total_seats = int(p.get("total_seats", 200))

            if not tail or not model:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error="Tail number and model are required",
                    status_code=400,
                )
                continue

            if tail in seen_tails:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Duplicate tail number '{tail}' within batch",
                    status_code=400,
                )
                continue

            seen_tails.add(tail)
            candidates.append({
                "event_id": eid,
                "tail_number": tail,
                "model": model,
                "total_seats": total_seats,
            })

        if not candidates:
            return results

        # 2. Check existing tail numbers in DB
        tails_to_check = [c["tail_number"] for c in candidates]
        existing_rows = await apg_conn.fetch(
            "SELECT tail_number FROM plane WHERE tail_number = ANY($1::text[]);",
            tails_to_check,
        )
        existing_tails = {r["tail_number"] for r in existing_rows}

        valid_candidates = []
        for c in candidates:
            eid = c["event_id"]
            tail = c["tail_number"]
            if tail in existing_tails:
                results[eid] = WriteResult(
                    event_id=eid,
                    status=WriteResultStatus.ERROR,
                    error=f"Plane with tail number '{tail}' already exists",
                    status_code=400,
                )
            else:
                valid_candidates.append(c)

        if not valid_candidates:
            return results

        # 3. Prefetch plane IDs in a single query
        num_planes = len(valid_candidates)
        plane_seq = await apg_conn.fetch(
            "SELECT nextval('plane_plane_id_seq') AS id FROM generate_series(1, $1);",
            num_planes,
        )

        plane_records = []
        total_seats_needed = sum(c["total_seats"] for c in valid_candidates)

        for i, c in enumerate(valid_candidates):
            pid = plane_seq[i]["id"]
            c["plane_id"] = pid
            plane_records.append((
                pid,
                c["tail_number"],
                c["model"],
                c["total_seats"],
            ))

        # 4. Insert planes via COPY
        await apg_conn.copy_records_to_table(
            "plane",
            records=plane_records,
            columns=["plane_id", "tail_number", "model", "total_seats"],
        )

        # 5. Prefetch seat IDs and insert physical seats via COPY
        if total_seats_needed > 0:
            seat_seq = await apg_conn.fetch(
                "SELECT nextval('seat_seat_id_seq') AS id FROM generate_series(1, $1);",
                total_seats_needed,
            )

            seat_records = []
            seat_cursor = 0
            cols = ["A", "B", "C", "D", "E", "F"]

            for c in valid_candidates:
                pid = c["plane_id"]
                n_seats = c["total_seats"]
                for s in range(1, n_seats + 1):
                    sid = seat_seq[seat_cursor]["id"]
                    seat_cursor += 1
                    row_num = ((s - 1) // 6) + 1
                    col_letter = cols[(s - 1) % 6]
                    seat_number = f"{row_num}{col_letter}"

                    if s <= 18:
                        seat_class = "FIRST"
                    elif s <= 48:
                        seat_class = "BUSINESS"
                    elif s <= 84:
                        seat_class = "PREMIUM_ECONOMY"
                    else:
                        seat_class = "ECONOMY"

                    seat_records.append((sid, pid, seat_number, seat_class))

            await apg_conn.copy_records_to_table(
                "seat",
                records=seat_records,
                columns=["seat_id", "plane_id", "seat_number", "seat_class"],
            )

        # 6. Post-commit cache invalidation
        post_commit_actions.append(
            lambda: cache_service.delete_pattern("cache:admin:planes*")
        )

        # 7. Build success results
        for c in valid_candidates:
            eid = c["event_id"]
            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "plane_id": c["plane_id"],
                    "tail_number": c["tail_number"],
                    "model": c["model"],
                    "total_seats": c["total_seats"],
                },
                status_code=201,
            )

        return results

    # =========================================================================
    # Batch & Single Execution Orchestrators
    # =========================================================================

    async def execute_and_commit_batch(
        self, batch: list[tuple[str, str, str | None]]
    ) -> list[WriteResult]:
        """Execute all events within a SINGLE batched database transaction through PgBouncer

        using bulk execution patterns (COPY for insert-only, UNNEST for upserts/bulk updates,
        sorted primary key locking), avoiding individual statements inside transactions,
        and deferring Redis cache/Lua operations until post-commit.
        """
        if not batch:
            return []

        ack_ids: list[str] = []
        parsed_events: list[tuple[str, str, WriteEvent]] = []
        results_map: dict[str, WriteResult] = {}
        timing_map: dict[str, tuple[float, float]] = {}

        t_pre_start = time.perf_counter()

        for msg_id, event_id, data_str in batch:
            ack_ids.append(msg_id)
            if not data_str:
                continue
            try:
                event_dict = orjson.loads(data_str)
                event = WriteEvent.model_validate(event_dict)
                parsed_events.append((msg_id, event_id, event))
            except Exception as ex:
                results_map[event_id] = WriteResult(
                    event_id=event_id,
                    status=WriteResultStatus.ERROR,
                    error=f"Stream processing failed: {ex}",
                    status_code=400,
                )
                timing_map[event_id] = (t_pre_start, time.perf_counter())

        # Precompute bcrypt password hashes outside DB transaction to minimize lock hold duration
        pw_hash_tasks: list[tuple[str, bytes, int]] = []
        for msg_id, event_id, event in parsed_events:
            if event.action == WriteAction.CREATE_CUSTOMER:
                pw = event.payload.get("password", "")
                rounds = 6 if settings.DEBUG else 12
                pw_hash_tasks.append((event_id, pw.encode("utf-8")[:72], rounds))

        def _safe_hash(b: bytes, r: int) -> str:
            try:
                return bcrypt.hashpw(b, bcrypt.gensalt(rounds=r)).decode("utf-8")
            except Exception:
                return ""

        pw_hashes: dict[str, str] = {}
        if pw_hash_tasks:
            hashes = await asyncio.gather(*[
                asyncio.to_thread(_safe_hash, b, r)
                for _, b, r in pw_hash_tasks
            ])
            for idx, (eid, _, _) in enumerate(pw_hash_tasks):
                pw_hashes[eid] = hashes[idx]

        # Group valid events by action
        events_by_action: dict[WriteAction, list[dict[str, Any]]] = {}
        for msg_id, event_id, event in parsed_events:
            item = {"msg_id": msg_id, "event_id": event_id, "payload": event.payload}
            if event.action == WriteAction.UPDATE_FLIGHT_STATUS:
                item["flight_id"] = event.payload.get("flight_id")
                item["status"] = event.payload.get("status")
            elif event.action == WriteAction.UPDATE_CUSTOMER:
                item["customer_id"] = event.payload.get("customer_id")
            elif event.action == WriteAction.CANCEL_BOOKING:
                item["booking_id"] = event.payload.get("booking_id")
            elif event.action == WriteAction.CONFIRM_BOOKING:
                item["booking_id"] = event.payload.get("booking_id")
            events_by_action.setdefault(event.action, []).append(item)

        t_tx_start_perf = time.perf_counter()
        t_tx_start_wall = datetime.now(timezone.utc)
        self.on_tx_start(batch, t_tx_start_perf, t_tx_start_wall)

        post_commit_actions: list[Callable[[], Any]] = []

        try:
            async with async_session_factory() as session:
                async with session.begin():
                    conn = await session.connection()
                    raw = await conn.get_raw_connection()
                    apg_conn: asyncpg.Connection = raw.driver_connection

                    # 1. Update Flight Status (UNNEST, sorted by flight_id)
                    if WriteAction.UPDATE_FLIGHT_STATUS in events_by_action:
                        res = await self._bulk_update_flight_status(
                            apg_conn,
                            events_by_action[WriteAction.UPDATE_FLIGHT_STATUS],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 2. Create Flight (COPY flight_seat)
                    if WriteAction.CREATE_FLIGHT in events_by_action:
                        res = await self._bulk_create_flight(
                            apg_conn,
                            events_by_action[WriteAction.CREATE_FLIGHT],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 3. Create Customer (COPY customer)
                    if WriteAction.CREATE_CUSTOMER in events_by_action:
                        res = await self._bulk_create_customer(
                            apg_conn,
                            events_by_action[WriteAction.CREATE_CUSTOMER],
                            pw_hashes,
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 3b. Update Customer (UNNEST customer)
                    if WriteAction.UPDATE_CUSTOMER in events_by_action:
                        res = await self._bulk_update_customer(
                            apg_conn,
                            events_by_action[WriteAction.UPDATE_CUSTOMER],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 4. Cancel Booking (UNNEST booking, ticket, flight_seat) - release seats and locks first
                    if WriteAction.CANCEL_BOOKING in events_by_action:
                        res = await self._bulk_cancel_booking(
                            apg_conn,
                            events_by_action[WriteAction.CANCEL_BOOKING],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 5. Create Booking (sorted FOR UPDATE, UNNEST flight_seat, COPY booking & ticket)
                    if WriteAction.CREATE_BOOKING in events_by_action:
                        res = await self._bulk_create_booking(
                            apg_conn,
                            events_by_action[WriteAction.CREATE_BOOKING],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 6. Confirm Booking (sorted FOR UPDATE, UNNEST booking & ticket)
                    if WriteAction.CONFIRM_BOOKING in events_by_action:
                        res = await self._bulk_confirm_booking(
                            apg_conn,
                            events_by_action[WriteAction.CONFIRM_BOOKING],
                            post_commit_actions,
                        )
                        results_map.update(res)

                    # 7. Create Plane (COPY plane & seat)
                    if WriteAction.CREATE_PLANE in events_by_action:
                        res = await self._bulk_create_plane(
                            apg_conn,
                            events_by_action[WriteAction.CREATE_PLANE],
                            post_commit_actions,
                        )
                        results_map.update(res)

        except Exception as tx_ex:
            logger.exception(
                f"Transaction failed during execute_and_commit_batch: {tx_ex}"
            )
            # Fallback: if multi-item batch failed, process each unhandled event individually
            if len(batch) > 1:
                for msg_id, event_id, raw_data in batch:
                    if event_id in results_map:
                        continue
                    try:
                        if not raw_data:
                            raise ValueError("Empty event data")
                        parsed = orjson.loads(raw_data)
                        ev = WriteEvent.model_validate(parsed)
                        async with async_session_factory() as single_sess:
                            async with single_sess.begin():
                                res = await self.process_event_in_session(single_sess, ev)
                                results_map[event_id] = res
                    except Exception as single_ex:
                        status_code, err_msg = self._map_exception_to_status_and_message(single_ex)
                        results_map[event_id] = WriteResult(
                            event_id=event_id,
                            status=WriteResultStatus.ERROR,
                            error=f"Stream processing failed: {err_msg}",
                            status_code=status_code,
                        )
            else:
                status_code, err_msg = self._map_exception_to_status_and_message(tx_ex)
                for msg_id, event_id, _ in batch:
                    if event_id not in results_map:
                        results_map[event_id] = WriteResult(
                            event_id=event_id,
                            status=WriteResultStatus.ERROR,
                            error=f"Stream processing failed: {err_msg}",
                            status_code=status_code,
                        )

        t_tx_complete_perf = time.perf_counter()
        t_tx_complete_wall = datetime.now(timezone.utc)
        self.on_tx_complete(batch, t_tx_complete_perf, t_tx_complete_wall)

        # Notify on_item_processed hooks for benchmark timing
        for msg_id, event_id, _ in batch:
            res = results_map.get(event_id)
            if not res:
                res = WriteResult(
                    event_id=event_id,
                    status=WriteResultStatus.ERROR,
                    error="Unprocessed event",
                    status_code=500,
                )
                results_map[event_id] = res
            t_start, t_end = timing_map.get(
                event_id, (t_tx_start_perf, t_tx_complete_perf)
            )
            self.on_item_processed(event_id, res, t_start, t_end, msg_id)

        # 1. Execute post-commit tasks (Redis cache invalidation & Lua updates)
        if post_commit_actions:
            tasks = []
            for act in post_commit_actions:
                try:
                    res = act()
                    if asyncio.iscoroutine(res):
                        tasks.append(res)
                except Exception as e:
                    logger.warning(f"Error invoking post-commit action: {e}")
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.warning(f"Post-commit task error: {r}")

        # 2. Pipeline publish all results and XACK so waiting clients resume with updated cache
        ordered_results = [
            results_map[eid] for _, eid, _ in batch if eid in results_map
        ]
        if ordered_results or ack_ids:
            redis = get_redis_client()
            pipe = redis.pipeline()
            for r in ordered_results:
                res_json = r.model_dump_json()
                pipe.set(f"write_res:{r.event_id}", res_json, ex=120)
                pipe.publish(f"write_res_chan:{r.event_id}", res_json)
            if ack_ids:
                pipe.xack(self.stream_name, self.group_name, *ack_ids)
            await pipe.execute()

        return ordered_results

    async def process_event_in_session(
        self, session: Any, event: WriteEvent
    ) -> WriteResult:
        """Process a single event within an already-opened database session using bulk patterns."""
        conn = await session.connection()
        raw = await conn.get_raw_connection()
        apg_conn = raw.driver_connection

        post_commit_actions: list[Callable[[], Any]] = []
        try:
            match event.action:
                case WriteAction.CREATE_CUSTOMER:
                    pw = event.payload.get("password", "")
                    rounds = 6 if settings.DEBUG else 12
                    pw_hash = await asyncio.to_thread(
                        lambda: bcrypt.hashpw(
                            pw.encode("utf-8")[:72], bcrypt.gensalt(rounds=rounds)
                        ).decode("utf-8")
                    )
                    res_map = await self._bulk_create_customer(
                        apg_conn,
                        [{"event_id": event.event_id, "payload": event.payload}],
                        {event.event_id: pw_hash},
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.UPDATE_CUSTOMER:
                    res_map = await self._bulk_update_customer(
                        apg_conn,
                        [{
                            "event_id": event.event_id,
                            "customer_id": event.payload.get("customer_id"),
                            "payload": event.payload,
                        }],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.CREATE_FLIGHT:
                    res_map = await self._bulk_create_flight(
                        apg_conn,
                        [{"event_id": event.event_id, "payload": event.payload}],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.UPDATE_FLIGHT_STATUS:
                    res_map = await self._bulk_update_flight_status(
                        apg_conn,
                        [{
                            "event_id": event.event_id,
                            "flight_id": event.payload.get("flight_id"),
                            "status": event.payload.get("status"),
                        }],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.CREATE_BOOKING:
                    res_map = await self._bulk_create_booking(
                        apg_conn,
                        [{"event_id": event.event_id, "payload": event.payload}],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.CANCEL_BOOKING:
                    res_map = await self._bulk_cancel_booking(
                        apg_conn,
                        [{
                            "event_id": event.event_id,
                            "booking_id": event.payload.get("booking_id"),
                        }],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.CONFIRM_BOOKING:
                    res_map = await self._bulk_confirm_booking(
                        apg_conn,
                        [{
                            "event_id": event.event_id,
                            "booking_id": event.payload.get("booking_id"),
                            "payload": event.payload,
                        }],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case WriteAction.CREATE_PLANE:
                    res_map = await self._bulk_create_plane(
                        apg_conn,
                        [{
                            "event_id": event.event_id,
                            "payload": event.payload,
                        }],
                        post_commit_actions,
                    )
                    res = res_map.get(event.event_id)
                case _:
                    return WriteResult(
                        event_id=event.event_id,
                        status=WriteResultStatus.ERROR,
                        error=f"Unknown write action: {event.action}",
                        status_code=400,
                    )

            if post_commit_actions:
                tasks = []
                for act in post_commit_actions:
                    try:
                        r = act()
                        if asyncio.iscoroutine(r):
                            tasks.append(r)
                    except Exception as e:
                        logger.warning(f"Error in post-commit action: {e}")
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            return res or WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error="Operation failed",
                status_code=500,
            )
        except ValueError as ve:
            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error=str(ve),
                status_code=400,
            )
        except Exception as ex:
            status_code, err_msg = self._map_exception_to_status_and_message(ex)
            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error=err_msg,
                status_code=status_code,
            )

    async def process_event(self, event: WriteEvent) -> WriteResult:
        """Dispatch a single event inside an isolated async DB transaction routed via PgBouncer."""
        results = await self.execute_and_commit_batch(
            [("single", event.event_id, event.model_dump_json())]
        )
        return results[0] if results else WriteResult(
            event_id=event.event_id,
            status=WriteResultStatus.ERROR,
            error="Execution returned no result",
            status_code=500,
        )

    async def process_one_batch(
        self, count: int | None = None, block: int | None = None
    ) -> int:
        """Dynamic micro-batching worker ingestion & processing:

        - Inspects queue depth (unread messages backlog in Redis Stream).
        - If queue depth is low (<= queue_depth_threshold):
            Flushes immediately with minimal batch size and NO linger accumulation delay.
        - If queue depth rises (> queue_depth_threshold):
            Batches up to max_batch_size messages, accumulating incoming events
            within batch_timeout_ms, before committing in a single bulk transaction.
        """
        queue_depth = await self.get_queue_depth()
        is_backlog = queue_depth > self.queue_depth_threshold

        if count is not None:
            target_batch_size = count
            read_block = block if block is not None else self.block_ms
            linger_ms = 0
        elif not is_backlog:
            target_batch_size = max(
                self.min_batch_size,
                min(queue_depth if queue_depth > 0 else self.min_batch_size, self.max_batch_size),
            )
            read_block = block if block is not None else self.block_ms
            linger_ms = 0
        else:
            target_batch_size = self.max_batch_size
            read_block = block if block is not None else 0
            linger_ms = self.batch_timeout_ms

        t_received_perf = time.perf_counter()
        t_received_wall = datetime.now(timezone.utc)
        batch = await self._read_stream_messages(
            count=target_batch_size, block=read_block
        )

        if not batch:
            return 0

        self.on_batch_received(batch, t_received_perf, t_received_wall)

        if linger_ms > 0 and len(batch) < target_batch_size:
            loop = asyncio.get_event_loop()
            deadline = loop.time() + (linger_ms / 1000.0)

            while len(batch) < target_batch_size:
                remaining_sec = deadline - loop.time()
                if remaining_sec <= 0:
                    break
                remaining_count = target_batch_size - len(batch)
                poll_block = max(1, int(remaining_sec * 1000))
                more = await self._read_stream_messages(
                    count=remaining_count, block=poll_block
                )
                if not more:
                    break
                self.on_batch_received(
                    more, time.perf_counter(), datetime.now(timezone.utc)
                )
                batch.extend(more)

        await self.execute_and_commit_batch(batch)
        return len(batch)

    async def _consumer_loop(self) -> None:
        """Task A: High-throughput decoupled Redis Stream consumer loop.

        Continuously reads batches from Redis Stream and enqueues them into the internal buffer queue.
        """
        while self.is_running:
            try:
                read_block = self.block_ms if self._buffer_queue.empty() else 0
                t_received_perf = time.perf_counter()
                t_received_wall = datetime.now(timezone.utc)

                batch = await self._read_stream_messages(
                    count=self.max_batch_size, block=read_block
                )

                if not batch:
                    continue

                self.on_batch_received(batch, t_received_perf, t_received_wall)
                await self._buffer_queue.put(batch)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream worker consumer loop: {e}")
                await asyncio.sleep(0.05)

    async def _drain_buffer_queue(self) -> None:
        """Drain any remaining buffered batches on worker shutdown."""
        drain_batch: list[tuple[str, str, str | None]] = []
        while not self._buffer_queue.empty():
            try:
                chunk = self._buffer_queue.get_nowait()
                if chunk is not None:
                    drain_batch.extend(chunk)
                self._buffer_queue.task_done()
            except asyncio.QueueEmpty:
                break
            if len(drain_batch) >= self.max_batch_size:
                try:
                    await self.execute_and_commit_batch(drain_batch)
                except Exception as e:
                    logger.error(f"Error executing shutdown drain batch: {e}")
                drain_batch = []
        if drain_batch:
            try:
                await self.execute_and_commit_batch(drain_batch)
            except Exception as e:
                logger.error(f"Error executing shutdown drain batch: {e}")

    async def _flusher_loop(self) -> None:
        """Task B: PostgreSQL bulk flush worker.

        Dequeues batches from internal buffer queue and commits transactions via PgBouncer.
        """
        loop = asyncio.get_event_loop()
        while self.is_running or not self._buffer_queue.empty():
            try:
                try:
                    chunk = await asyncio.wait_for(self._buffer_queue.get(), timeout=0.2)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

                if chunk is None:
                    break

                batch = list(chunk)

                # Drain all immediately available chunks from buffer queue up to max_batch_size
                while len(batch) < self.max_batch_size and not self._buffer_queue.empty():
                    try:
                        next_chunk = self._buffer_queue.get_nowait()
                        if next_chunk is None:
                            break
                        batch.extend(next_chunk)
                        self._buffer_queue.task_done()
                    except asyncio.QueueEmpty:
                        break

                # Dynamic micro-batching:
                # If backlog exists (queue depth > threshold or items waiting in buffer),
                # accumulate incoming chunks within batch_timeout_ms up to max_batch_size.
                if len(batch) < self.max_batch_size:
                    q_depth = self._buffer_queue.qsize()
                    if q_depth > 0 or (await self.get_queue_depth()) > self.queue_depth_threshold:
                        deadline = loop.time() + (self.batch_timeout_ms / 1000.0)
                        while len(batch) < self.max_batch_size:
                            remaining = deadline - loop.time()
                            if remaining <= 0:
                                break
                            try:
                                next_chunk = await asyncio.wait_for(
                                    self._buffer_queue.get(), timeout=remaining
                                )
                                if next_chunk is None:
                                    break
                                batch.extend(next_chunk)
                                self._buffer_queue.task_done()
                            except asyncio.TimeoutError:
                                break

                try:
                    await self.execute_and_commit_batch(batch)
                except Exception as e:
                    logger.error(f"Error in stream worker flusher batch execution: {e}")
                finally:
                    self._buffer_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream worker flusher loop: {e}")
                await asyncio.sleep(0.05)

        try:
            await asyncio.shield(self._drain_buffer_queue())
        except Exception as e:
            logger.warning(f"Drain buffer exception on shutdown: {e}")

    async def run_loop(self) -> None:
        """Continuous consumer worker utilizing dynamic micro-batching.

        Supports both high-throughput adaptive batching and decoupled (Task A ingestion / Task B flusher)
        architectures configurable via STREAM_WORKER_FLUSH_STRATEGY.
        """
        self.is_running = True
        await self.init_consumer_group()
        self._buffer_queue = asyncio.Queue()
        logger.info(
            f"StreamWorker started on {self.stream_name} (consumer={self.consumer_name}, "
            f"strategy={self.flush_strategy}, queue_threshold={self.queue_depth_threshold}, "
            f"min_batch={self.min_batch_size}, max_batch={self.max_batch_size}, "
            f"batch_timeout={self.batch_timeout_ms}ms, block={self.block_ms}ms)..."
        )

        if self.flush_strategy == "decoupled":
            self._consumer_task = asyncio.create_task(
                self._consumer_loop(), name=f"{self.consumer_name}-consumer"
            )
            self._flusher_task = asyncio.create_task(
                self._flusher_loop(), name=f"{self.consumer_name}-flusher"
            )

            try:
                await asyncio.gather(self._consumer_task, asyncio.shield(self._flusher_task))
            except asyncio.CancelledError:
                self.stop()
                if self._flusher_task and not self._flusher_task.done():
                    try:
                        await self._flusher_task
                    except Exception:
                        pass
                raise
            finally:
                self.is_running = False
                logger.info(f"StreamWorker {self.consumer_name} terminated.")
        else:
            # High-throughput dynamic micro-batching loop
            while self.is_running:
                try:
                    await self.process_one_batch()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in stream worker loop: {e}")
                    await asyncio.sleep(0.5)

            self.is_running = False
            logger.info(f"StreamWorker {self.consumer_name} terminated.")

    def stop(self) -> None:
        """Signal stop to consumer and allow flusher to drain remaining items."""
        self.is_running = False
        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
        try:
            self._buffer_queue.put_nowait(None)
        except Exception:
            pass


stream_worker = StreamWorker()
