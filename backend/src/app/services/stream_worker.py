import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import time
from typing import Any, Callable
import uuid
import asyncpg
import bcrypt
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
        try:
            read_res = await redis.xreadgroup(
                self.group_name,
                self.consumer_name,
                {self.stream_name: ">"},
                count=count,
                block=block,
            )
        except Exception as e:
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

        for it in items:
            fid = it["flight_id"]
            st = it["status"]
            status_by_flight[fid] = st
            events_by_flight.setdefault(fid, []).append(it["event_id"])

        # Sort Batch Updates by Primary Key to prevent row-lock waits and transient deadlocks
        sorted_flight_ids = sorted(status_by_flight.keys())
        sorted_statuses = [status_by_flight[fid] for fid in sorted_flight_ids]

        # Bulk execute via UNNEST
        updated_rows = await apg_conn.fetch(
            """
            UPDATE flight AS f
            SET status = v.status
            FROM (
                SELECT * FROM UNNEST($1::bigint[], $2::text[]) AS t(flight_id, status)
            ) AS v
            WHERE f.flight_id = v.flight_id
            RETURNING f.flight_id, f.flight_number, f.status;
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
                    "status": row["status"],
                }
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.SUCCESS,
                        data=res_data,
                        status_code=200,
                    )
                post_commit_actions.append(
                    lambda f_id=fid: cache_service.delete(f"cache:flight:{f_id}")
                )
            else:
                for eid in ev_ids:
                    results[eid] = WriteResult(
                        event_id=eid,
                        status=WriteResultStatus.ERROR,
                        error=f"Flight {fid} not found",
                        status_code=400,
                    )

        if updated_rows:
            post_commit_actions.append(
                lambda: cache_service.delete_pattern("cache:search:*")
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
                    status_code=400,
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
                    status_code=400,
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
            post_commit_actions.append(
                lambda cid=c_id: cache_service.delete(f"cache:customer:{cid}")
            )
            post_commit_actions.append(
                lambda em=email: cache_service.delete(f"cache:customer:email:{em}")
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
                    status_code=400,
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
                    status_code=400,
                )
                continue

            valid_bookings.append(it)

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
            total_amt = sum(Decimal(str(item["price"])) for item in payload["tickets"])
            code = f"BK-{uuid.uuid4().hex[:8].upper()}"
            b["booking_code"] = code
            b["total_amount"] = total_amt
            b["booking_id"] = b_id

            booking_records.append((
                b_id,
                code,
                cid,
                total_amt,
                "PAID",
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
                price = Decimal(str(item["price"]))

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

        # 9. Format response results
        for b in valid_bookings:
            eid = b["event_id"]
            b_id = b["booking_id"]
            results[eid] = WriteResult(
                event_id=eid,
                status=WriteResultStatus.SUCCESS,
                data={
                    "booking_id": b_id,
                    "booking_code": b["booking_code"],
                    "customer_id": b["payload"]["customer_id"],
                    "total_amount": str(b["total_amount"]),
                    "status": "PAID",
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

        # 1. Prefetch all flight IDs in a single query
        f_seq = await apg_conn.fetch(
            "SELECT nextval('flight_flight_id_seq') AS id FROM generate_series(1, $1);",
            num_flights,
        )

        # 2. Prepare and bulk insert flights via COPY ... FROM STDIN
        flight_records = []
        plane_ids_set: set[int] = set()

        for i, it in enumerate(items):
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
            plane_ids_set.add(plane_id)

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
        for i, it in enumerate(items):
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

        # 5. Invalidate search cache once post-commit
        post_commit_actions.append(
            lambda: cache_service.delete_pattern("cache:search:*")
        )

        # 6. Build response results
        for i, it in enumerate(items):
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
                event_dict = json.loads(data_str)
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
                pw_hash_tasks.append((event_id, pw.encode("utf-8"), rounds))

        pw_hashes: dict[str, str] = {}
        if pw_hash_tasks:
            hashes = await asyncio.gather(*[
                asyncio.to_thread(
                    lambda b=b, r=r: bcrypt.hashpw(b, bcrypt.gensalt(rounds=r)).decode("utf-8")
                )
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
            elif event.action == WriteAction.CANCEL_BOOKING:
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

        except Exception as tx_ex:
            logger.exception(
                f"Transaction failed during execute_and_commit_batch: {tx_ex}"
            )
            for msg_id, event_id, _ in batch:
                if event_id not in results_map:
                    results_map[event_id] = WriteResult(
                        event_id=event_id,
                        status=WriteResultStatus.ERROR,
                        error=f"Stream processing failed: {tx_ex}",
                        status_code=500,
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

        # Execute post-commit background tasks (Redis cache invalidation & Lua updates)
        for act in post_commit_actions:
            try:
                res = act()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Error executing post-commit action: {e}")

        # Batch publish all results and XACK in a single Redis pipeline
        ordered_results = [
            results_map[eid] for _, eid, _ in batch if eid in results_map
        ]
        if ordered_results:
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
                            pw.encode("utf-8"), bcrypt.gensalt(rounds=rounds)
                        ).decode("utf-8")
                    )
                    res_map = await self._bulk_create_customer(
                        apg_conn,
                        [{"event_id": event.event_id, "payload": event.payload}],
                        {event.event_id: pw_hash},
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
                case _:
                    return WriteResult(
                        event_id=event.event_id,
                        status=WriteResultStatus.ERROR,
                        error=f"Unknown write action: {event.action}",
                        status_code=400,
                    )

            for act in post_commit_actions:
                r = act()
                if asyncio.iscoroutine(r):
                    await r

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
            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error=str(ex),
                status_code=500,
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

    async def run_loop(self) -> None:
        """Continuous consumer loop utilizing dynamic micro-batching."""
        self.is_running = True
        await self.init_consumer_group()
        logger.info(
            f"StreamWorker started on {self.stream_name} "
            f"(queue_threshold={self.queue_depth_threshold}, min_batch={self.min_batch_size}, "
            f"max_batch={self.max_batch_size}, batch_timeout={self.batch_timeout_ms}ms, "
            f"block={self.block_ms}ms)..."
        )

        while self.is_running:
            try:
                await self.process_one_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream worker loop: {e}")
                await asyncio.sleep(0.5)

        logger.info("StreamWorker loop terminated.")

    def stop(self) -> None:
        self.is_running = False


stream_worker = StreamWorker()
