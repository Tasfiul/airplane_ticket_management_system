"""Redis-backed atomic flight seat inventory management using Redis Hashes & Lua scripts.

Provides sub-2ms atomic seat reservation and live status tracking without cache invalidation storms.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid
import orjson
from sqlalchemy import select
from app.core.config import settings
from app.core.redis import get_redis_client
from app.models.flight_seat import FlightSeat
from app.models.enums import FlightSeatStatus
from app.models.seat import Seat

# Atomic Lua script for reserving one or more seats in a flight's seat Hash
# KEYS[1]: flight:{flight_id}:seats
# ARGV[1...N]: flight_seat_ids
# Returns:
#   1: All seats were AVAILABLE and are now atomically marked as BOOKED
#   0: One or more seats are already BOOKED / unavailable
#  -1: Flight seat hash not initialized in Redis (cache miss)
RESERVE_SEATS_LUA = """
local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then
    return -1
end

for i = 1, #ARGV do
    local current = redis.call('HGET', KEYS[1], ARGV[i])
    if not current or current ~= 'AVAILABLE' then
        return 0
    end
end

for i = 1, #ARGV do
    redis.call('HSET', KEYS[1], ARGV[i], 'BOOKED')
end
return 1
"""

# Atomic Lua script for releasing seats back to AVAILABLE (e.g. on cancellation)
RELEASE_SEATS_LUA = """
local exists = redis.call('EXISTS', KEYS[1])
if exists == 1 then
    for i = 1, #ARGV do
        redis.call('HSET', KEYS[1], ARGV[i], 'AVAILABLE')
    end
    return 1
end
return 0
"""

# Atomic Lua script for placing an ephemeral hold on seats
# Returns: 1 (all held), 0 (conflict: seat already booked or held), -1 (hash miss)
HOLD_SEATS_LUA = """
local exists = redis.call('EXISTS', KEYS[1])
if exists == 0 then
    return -1
end

for i = 1, #ARGV do
    local current = redis.call('HGET', KEYS[1], ARGV[i])
    if not current or current ~= 'AVAILABLE' then
        return 0
    end
end

for i = 1, #ARGV do
    redis.call('HSET', KEYS[1], ARGV[i], 'HELD')
end
return 1
"""

# Atomic Lua script to release held seats back to AVAILABLE
RELEASE_HOLD_SEATS_LUA = """
local exists = redis.call('EXISTS', KEYS[1])
if exists == 1 then
    for i = 1, #ARGV do
        local current = redis.call('HGET', KEYS[1], ARGV[i])
        if current == 'HELD' then
            redis.call('HSET', KEYS[1], ARGV[i], 'AVAILABLE')
        end
    end
    return 1
end
return 0
"""

# Atomic Lua script to confirm held seats transitioning HELD -> BOOKED
CONFIRM_HELD_SEATS_LUA = """
local exists = redis.call('EXISTS', KEYS[1])
if exists == 1 then
    for i = 1, #ARGV do
        redis.call('HSET', KEYS[1], ARGV[i], 'BOOKED')
    end
    return 1
end
return 0
"""


class SeatInventoryService:
    """Manages seat availability maps directly in Redis Hash structures with atomic Lua operations."""

    def __init__(self):
        self.reserve_script_sha = None
        self.release_script_sha = None

    def _seats_hash_key(self, flight_id: int) -> str:
        return f"flight:{flight_id}:seats"

    def _seats_meta_key(self, flight_id: int) -> str:
        return f"flight:{flight_id}:seat_meta"

    async def _publish_seat_event(
        self,
        flight_id: int,
        flight_seat_ids: Sequence[int],
        status: str,
    ) -> None:
        """Publish real-time seat status transition event to Redis Pub/Sub."""
        try:
            redis = get_redis_client()
            channel = f"flight:{flight_id}:seats:events"
            payload = orjson.dumps({
                "flight_id": flight_id,
                "status": status,
                "flight_seat_ids": [int(sid) for sid in flight_seat_ids],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }).decode("utf-8")
            await redis.publish(channel, payload)
        except Exception:
            pass

    async def get_or_load_seat_map(
        self,
        flight_id: int,
        db_session_or_conn: Any = None,
        ttl: int = 1800,
        return_hit_status: bool = False,
    ) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], bool]:
        """Fetch seat map from Redis Hash.

        On cache miss, loads from PostgreSQL via PgBouncer and populates the Redis Hash with TTL.
        Uses a single pipelined roundtrip to query both seat statuses and metadata concurrently.
        Supports lazy DB acquisition: if db_session_or_conn is None, a connection is checked out
        only on a cache miss.
        """
        redis = get_redis_client()
        hash_key = self._seats_hash_key(flight_id)
        meta_key = self._seats_meta_key(flight_id)

        # 1. Fast-path cache hit: fetch hash and metadata concurrently in 1 roundtrip
        pipe = redis.pipeline(transaction=False)
        pipe.hgetall(hash_key)
        pipe.get(meta_key)
        seats_status, meta_raw = await pipe.execute()

        if seats_status and meta_raw:
            try:
                meta_list = orjson.loads(meta_raw)
                # Merge live status from hash
                for item in meta_list:
                    s_id_str = str(item["flight_seat_id"])
                    if s_id_str in seats_status:
                        item["status"] = seats_status[s_id_str]
                if return_hit_status:
                    return meta_list, True
                return meta_list
            except Exception:
                pass

        # 2. Cache Miss: Fetch from PostgreSQL via PgBouncer (lazy acquisition if None)
        if db_session_or_conn is None:
            from app.core.database import async_session_factory
            async with async_session_factory() as session:
                return await self._fetch_and_populate_seat_map(
                    flight_id=flight_id,
                    db_session_or_conn=session,
                    hash_key=hash_key,
                    meta_key=meta_key,
                    ttl=ttl,
                    return_hit_status=return_hit_status,
                )

        return await self._fetch_and_populate_seat_map(
            flight_id=flight_id,
            db_session_or_conn=db_session_or_conn,
            hash_key=hash_key,
            meta_key=meta_key,
            ttl=ttl,
            return_hit_status=return_hit_status,
        )

    async def _fetch_and_populate_seat_map(
        self,
        flight_id: int,
        db_session_or_conn: Any,
        hash_key: str,
        meta_key: str,
        ttl: int,
        return_hit_status: bool,
    ) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], bool]:
        """Loads seat rows from DB, caches in Redis Hash, and returns formatted seat dicts."""
        redis = get_redis_client()
        if hasattr(db_session_or_conn, "fetch"):
            # asyncpg Connection
            pg_rows = await db_session_or_conn.fetch(
                """
                SELECT fs.flight_seat_id, fs.flight_id, fs.seat_id, fs.status,
                       s.seat_number, s.seat_class
                FROM flight_seat fs
                JOIN seat s ON fs.seat_id = s.seat_id
                WHERE fs.flight_id = $1;
            """,
                flight_id,
            )
            rows = [dict(r) for r in pg_rows]
        else:
            # SQLAlchemy AsyncSession
            stmt = (
                select(
                    FlightSeat.flight_seat_id,
                    FlightSeat.flight_id,
                    FlightSeat.seat_id,
                    FlightSeat.status,
                    Seat.seat_number,
                    Seat.seat_class,
                )
                .join(Seat, FlightSeat.seat_id == Seat.seat_id)
                .where(FlightSeat.flight_id == flight_id)
            )
            res = await db_session_or_conn.execute(stmt)
            rows = [
                {
                    "flight_seat_id": r.flight_seat_id,
                    "flight_id": r.flight_id,
                    "seat_id": r.seat_id,
                    "status": r.status.value if hasattr(r.status, "value") else str(r.status),
                    "seat_number": r.seat_number,
                    "seat_class": r.seat_class,
                }
                for r in res.all()
            ]

        if not rows:
            if return_hit_status:
                return [], False
            return []

        # 3. Populate Redis Hash and metadata
        mapping = {
            str(r["flight_seat_id"]): (
                r["status"].value if hasattr(r["status"], "value") else str(r["status"])
            )
            for r in rows
        }
        pipe = redis.pipeline(transaction=False)
        pipe.hset(hash_key, mapping=mapping)
        pipe.expire(hash_key, ttl)
        pipe.set(meta_key, orjson.dumps(rows).decode("utf-8"), ex=ttl)
        await pipe.execute()

        if return_hit_status:
            return rows, False
        return rows

    async def get_or_load_seat_map_bytes(
        self,
        flight_id: int,
        db_session_or_conn: Any = None,
        ttl: int = 1800,
    ) -> bytes:
        """Fetch seat map from Redis Hash directly as JSON bytes, avoiding Pydantic validation overhead.

        On cache miss, loads from PostgreSQL via PgBouncer and populates the Redis Hash with TTL.
        """
        redis = get_redis_client()
        hash_key = self._seats_hash_key(flight_id)
        meta_key = self._seats_meta_key(flight_id)

        pipe = redis.pipeline(transaction=False)
        pipe.hgetall(hash_key)
        pipe.get(meta_key)
        seats_status, meta_raw = await pipe.execute()

        if seats_status and meta_raw:
            try:
                meta_list = orjson.loads(meta_raw)
                for item in meta_list:
                    s_id_str = str(item["flight_seat_id"])
                    if s_id_str in seats_status:
                        item["status"] = seats_status[s_id_str]
                return orjson.dumps(meta_list)
            except Exception:
                pass

        # Cache miss
        rows = await self.get_or_load_seat_map(
            flight_id=flight_id,
            db_session_or_conn=db_session_or_conn,
            ttl=ttl,
        )
        return orjson.dumps(rows)

    async def reserve_seats_atomic(
        self,
        flight_id: int,
        flight_seat_ids: Sequence[int],
        db_fallback: Any = None,
    ) -> bool:
        """Atomically reserve one or more seats in Redis Hash in <2ms using Lua.

        Returns True if reservation succeeded, False if already booked.
        """
        redis = get_redis_client()
        hash_key = self._seats_hash_key(flight_id)
        seat_id_strs = [str(sid) for sid in flight_seat_ids]

        # Execute Lua reservation
        res = await redis.eval(RESERVE_SEATS_LUA, 1, hash_key, *seat_id_strs)

        if res == -1:
            # Hash not initialized in Redis: populate first (lazy DB fallback if db_fallback is None)
            await self.get_or_load_seat_map(flight_id, db_fallback)
            res = await redis.eval(
                RESERVE_SEATS_LUA, 1, hash_key, *seat_id_strs
            )

        if res == 1:
            await self._publish_seat_event(flight_id, flight_seat_ids, "BOOKED")
            return True
        return False

    async def release_seats_atomic(
        self,
        flight_id: int,
        flight_seat_ids: Sequence[int],
    ) -> bool:
        """Atomically release seats back to AVAILABLE in Redis Hash."""
        redis = get_redis_client()
        hash_key = self._seats_hash_key(flight_id)
        seat_id_strs = [str(sid) for sid in flight_seat_ids]
        res = await redis.eval(RELEASE_SEATS_LUA, 1, hash_key, *seat_id_strs)
        if res == 1:
            await self._publish_seat_event(flight_id, flight_seat_ids, "AVAILABLE")
            return True
        return False

    async def hold_seats_atomic(
        self,
        flight_id: int,
        flight_seat_ids: Sequence[int],
        customer_id: int,
        ttl_seconds: int = 600,
        db_fallback: Any = None,
    ) -> tuple[bool, str, datetime | None]:
        """Atomically hold one or more seats in Redis Hash with TTL.

        Returns (True, hold_token, expires_at) if successful, or (False, "", None) on conflict.
        """
        redis = get_redis_client()
        hash_key = self._seats_hash_key(flight_id)
        seat_id_strs = [str(sid) for sid in flight_seat_ids]

        res = await redis.eval(HOLD_SEATS_LUA, 1, hash_key, *seat_id_strs)
        if res == -1:
            # Hash not initialized in Redis: populate first (lazy DB fallback if db_fallback is None)
            await self.get_or_load_seat_map(flight_id, db_fallback)
            res = await redis.eval(HOLD_SEATS_LUA, 1, hash_key, *seat_id_strs)

        if res != 1:
            return False, "", None

        hold_token = str(uuid.uuid4())
        now_dt = datetime.now(timezone.utc)
        expires_at = now_dt + timedelta(seconds=ttl_seconds)

        hold_data = {
            "hold_token": hold_token,
            "flight_id": flight_id,
            "customer_id": customer_id,
            "flight_seat_ids": [int(sid) for sid in flight_seat_ids],
            "expires_at": expires_at.isoformat(),
        }

        pipe = redis.pipeline()
        pipe.set(f"seat_hold:{hold_token}", orjson.dumps(hold_data).decode("utf-8"), ex=ttl_seconds)
        for sid in flight_seat_ids:
            pipe.set(f"seat_held:{flight_id}:{sid}", hold_token, ex=ttl_seconds)
        await pipe.execute()

        await self._publish_seat_event(flight_id, [int(sid) for sid in flight_seat_ids], "HELD")
        return True, hold_token, expires_at

    async def release_hold_atomic(self, hold_token: str) -> bool:
        """Atomically release held seats back to AVAILABLE."""
        redis = get_redis_client()
        raw = await redis.get(f"seat_hold:{hold_token}")
        if not raw:
            return False

        try:
            data = orjson.loads(raw)
            flight_id = data["flight_id"]
            seat_ids = [str(sid) for sid in data["flight_seat_ids"]]
            hash_key = self._seats_hash_key(flight_id)

            await redis.eval(RELEASE_HOLD_SEATS_LUA, 1, hash_key, *seat_ids)

            pipe = redis.pipeline()
            pipe.delete(f"seat_hold:{hold_token}")
            for sid in data["flight_seat_ids"]:
                pipe.delete(f"seat_held:{flight_id}:{sid}")
            await pipe.execute()
            await self._publish_seat_event(flight_id, [int(sid) for sid in data["flight_seat_ids"]], "AVAILABLE")
            return True
        except Exception:
            return False

    async def confirm_held_seats_atomic(self, hold_token: str) -> bool:
        """Atomically transition held seats to BOOKED."""
        redis = get_redis_client()
        raw = await redis.get(f"seat_hold:{hold_token}")
        if not raw:
            return False

        try:
            data = orjson.loads(raw)
            flight_id = data["flight_id"]
            seat_ids = [str(sid) for sid in data["flight_seat_ids"]]
            hash_key = self._seats_hash_key(flight_id)

            await redis.eval(CONFIRM_HELD_SEATS_LUA, 1, hash_key, *seat_ids)

            pipe = redis.pipeline()
            pipe.delete(f"seat_hold:{hold_token}")
            for sid in data["flight_seat_ids"]:
                pipe.delete(f"seat_held:{flight_id}:{sid}")
            await pipe.execute()
            await self._publish_seat_event(flight_id, [int(sid) for sid in data["flight_seat_ids"]], "BOOKED")
            return True
        except Exception:
            return False

    async def get_hold_details(self, hold_token: str) -> dict[str, Any] | None:
        """Retrieve active hold token metadata."""
        redis = get_redis_client()
        raw = await redis.get(f"seat_hold:{hold_token}")
        if not raw:
            return None
        return orjson.loads(raw)


seat_inventory = SeatInventoryService()
