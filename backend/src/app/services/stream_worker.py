import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import time
from typing import Any
import uuid
import bcrypt
from redis.exceptions import ResponseError
from sqlalchemy import insert, select, update
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

    through PgBouncer in transaction pooling mode, strictly decoupling HTTP handlers
    from direct DB write access.
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

    async def process_event_in_session(
        self, session: Any, event: WriteEvent
    ) -> WriteResult:
        """Process a single event within an already-opened database session."""
        try:
            match event.action:
                case WriteAction.CREATE_CUSTOMER:
                    result_data = await self._handle_create_customer(
                        session, event.payload
                    )
                case WriteAction.CREATE_FLIGHT:
                    result_data = await self._handle_create_flight(
                        session, event.payload
                    )
                case WriteAction.UPDATE_FLIGHT_STATUS:
                    result_data = (
                        await self._handle_update_flight_status(
                            session, event.payload
                        )
                    )
                case WriteAction.CREATE_BOOKING:
                    result_data = await self._handle_create_booking(
                        session, event.payload
                    )
                case WriteAction.CANCEL_BOOKING:
                    result_data = await self._handle_cancel_booking(
                        session, event.payload
                    )
                case _:
                    return WriteResult(
                        event_id=event.event_id,
                        status=WriteResultStatus.ERROR,
                        error=f"Unknown write action: {event.action}",
                        status_code=400,
                    )

            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.SUCCESS,
                data=result_data,
                status_code=201
                if event.action
                in (
                    WriteAction.CREATE_CUSTOMER,
                    WriteAction.CREATE_FLIGHT,
                    WriteAction.CREATE_BOOKING,
                )
                else 200,
            )

        except ValueError as ve:
            logger.warning(
                f"Validation error processing event {event.event_id}: {ve}"
            )
            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error=str(ve),
                status_code=400,
            )
        except Exception as ex:
            logger.exception(
                f"Unexpected error processing event {event.event_id}: {ex}"
            )
            return WriteResult(
                event_id=event.event_id,
                status=WriteResultStatus.ERROR,
                error=str(ex),
                status_code=500,
            )

    async def process_event(self, event: WriteEvent) -> WriteResult:
        """Dispatch a single event inside an isolated async DB transaction routed via PgBouncer."""
        async with async_session_factory() as session:
            async with session.begin():
                return await self.process_event_in_session(session, event)

    async def _handle_create_customer(
        self, session: Any, payload: dict[str, Any]
    ) -> dict[str, Any]:
        email = payload["email"]
        existing = await session.execute(
            select(Customer).where(Customer.email == email)
        )
        if existing.scalar_one_or_none():
            raise ValueError(f"Customer with email {email} already exists")

        pw_bytes = payload["password"].encode("utf-8")
        rounds = 6 if settings.DEBUG else 12
        pw_hash = await asyncio.to_thread(
            lambda: bcrypt.hashpw(pw_bytes, bcrypt.gensalt(rounds=rounds)).decode("utf-8")
        )

        dob = payload["date_of_birth"]
        if isinstance(dob, str):
            dob = date.fromisoformat(dob)

        customer = Customer(
            first_name=payload["first_name"],
            last_name=payload["last_name"],
            email=email,
            password_hash=pw_hash,
            city=payload.get("city"),
            postal_code=payload.get("postal_code"),
            division=payload.get("division"),
            country=payload.get("country"),
            date_of_birth=dob,
        )
        session.add(customer)
        await session.flush()

        # Invalidate related cache keys
        await cache_service.delete(f"cache:customer:{customer.customer_id}")
        await cache_service.delete(f"cache:customer:email:{email}")

        return {
            "customer_id": customer.customer_id,
            "first_name": customer.first_name,
            "last_name": customer.last_name,
            "email": customer.email,
            "city": customer.city,
            "postal_code": customer.postal_code,
            "division": customer.division,
            "country": customer.country,
            "date_of_birth": customer.date_of_birth.isoformat(),
            "created_at": customer.created_at.isoformat()
            if customer.created_at
            else datetime.utcnow().isoformat(),
        }

    async def _handle_create_flight(
        self, session: Any, payload: dict[str, Any]
    ) -> dict[str, Any]:
        dep_time = datetime.fromisoformat(payload["departure_time"])
        arr_time = datetime.fromisoformat(payload["arrival_time"])

        flight = Flight(
            flight_number=payload["flight_number"],
            plane_id=payload["plane_id"],
            departure_airport=payload["departure_airport"],
            arrival_airport=payload["arrival_airport"],
            departure_time=dep_time,
            arrival_time=arr_time,
            status=payload.get("status", FlightStatus.SCHEDULED),
        )
        session.add(flight)
        await session.flush()

        # Query seats on plane to generate flight_seat inventory
        seats_result = await session.execute(
            select(Seat.seat_id).where(Seat.plane_id == flight.plane_id)
        )
        seat_ids = seats_result.scalars().all()

        if seat_ids:
            seat_mappings = [
                {
                    "flight_id": flight.flight_id,
                    "seat_id": s_id,
                    "status": FlightSeatStatus.AVAILABLE,
                }
                for s_id in seat_ids
            ]
            await session.execute(insert(FlightSeat).values(seat_mappings))
            await session.flush()

        # Invalidate flight search caches (new flight available in searches)
        await cache_service.delete_pattern("cache:search:*")

        return {
            "flight_id": flight.flight_id,
            "flight_number": flight.flight_number,
            "plane_id": flight.plane_id,
            "departure_airport": flight.departure_airport,
            "arrival_airport": flight.arrival_airport,
            "departure_time": flight.departure_time.isoformat(),
            "arrival_time": flight.arrival_time.isoformat(),
            "status": flight.status,
            "seats_count": len(seat_ids),
            "created_at": flight.created_at.isoformat()
            if flight.created_at
            else datetime.utcnow().isoformat(),
        }

    async def _handle_update_flight_status(
        self, session: Any, payload: dict[str, Any]
    ) -> dict[str, Any]:
        flight_id = payload["flight_id"]
        new_status = payload["status"]

        flight = await session.get(Flight, flight_id)
        if not flight:
            raise ValueError(f"Flight {flight_id} not found")

        flight.status = new_status
        await session.flush()

        # Invalidate specific flight cache and search results
        await cache_service.delete(f"cache:flight:{flight_id}")
        await cache_service.delete_pattern("cache:search:*")

        return {
            "flight_id": flight.flight_id,
            "flight_number": flight.flight_number,
            "status": flight.status,
        }

    async def _handle_create_booking(
        self, session: Any, payload: dict[str, Any]
    ) -> dict[str, Any]:
        customer_id = payload["customer_id"]
        ticket_items = payload["tickets"]

        seat_ids = [item["flight_seat_id"] for item in ticket_items]

        # Lock the requested flight_seat rows with FOR UPDATE to prevent race conditions
        stmt = (
            select(FlightSeat)
            .where(FlightSeat.flight_seat_id.in_(seat_ids))
            .with_for_update()
        )
        res = await session.execute(stmt)
        seats = {s.flight_seat_id: s for s in res.scalars().all()}

        if len(seats) != len(seat_ids):
            missing = set(seat_ids) - set(seats.keys())
            raise ValueError(f"Flight seats not found: {missing}")

        for s_id, seat in seats.items():
            if seat.status != FlightSeatStatus.AVAILABLE:
                raise ValueError(
                    f"Flight seat {s_id} is already booked or unavailable"
                )

        # Mark seats as BOOKED
        for seat in seats.values():
            seat.status = FlightSeatStatus.BOOKED

        total_amount = sum(Decimal(str(item["price"])) for item in ticket_items)
        booking_code = f"BK-{uuid.uuid4().hex[:8].upper()}"

        booking = Booking(
            booking_code=booking_code,
            customer_id=customer_id,
            total_amount=total_amount,
            status=BookingStatus.PAID,
        )
        session.add(booking)
        await session.flush()

        tickets: list[dict[str, Any]] = []
        for item in ticket_items:
            t = Ticket(
                booking_id=booking.booking_id,
                flight_seat_id=item["flight_seat_id"],
                passenger_name=item["passenger_name"],
                price=Decimal(str(item["price"])),
                status=TicketStatus.ACTIVE,
            )
            session.add(t)
            await session.flush()
            tickets.append(
                {
                    "ticket_id": t.ticket_id,
                    "booking_id": booking.booking_id,
                    "flight_seat_id": t.flight_seat_id,
                    "passenger_name": t.passenger_name,
                    "price": str(t.price),
                    "status": t.status,
                    "issued_at": t.issued_at.isoformat()
                    if t.issued_at
                    else datetime.utcnow().isoformat(),
                }
            )

        # Atomically update seat status in Redis Hash (no cache invalidation storm)
        flight_ids = {s.flight_id for s in seats.values()}
        for fid in flight_ids:
            f_seat_ids = [
                s.flight_seat_id for s in seats.values() if s.flight_id == fid
            ]
            await seat_inventory.reserve_seats_atomic(fid, f_seat_ids)
            await cache_service.delete(f"cache:flight:{fid}")

        return {
            "booking_id": booking.booking_id,
            "booking_code": booking.booking_code,
            "customer_id": booking.customer_id,
            "total_amount": str(booking.total_amount),
            "status": booking.status,
            "created_at": booking.created_at.isoformat()
            if booking.created_at
            else datetime.utcnow().isoformat(),
            "tickets": tickets,
        }

    async def _handle_cancel_booking(
        self, session: Any, payload: dict[str, Any]
    ) -> dict[str, Any]:
        booking_id = payload["booking_id"]
        booking = await session.get(Booking, booking_id)
        if not booking:
            raise ValueError(f"Booking {booking_id} not found")

        booking.status = BookingStatus.CANCELLED

        tickets_res = await session.execute(
            select(Ticket).where(Ticket.booking_id == booking_id)
        )
        tickets = tickets_res.scalars().all()
        seat_ids = []
        for t in tickets:
            t.status = TicketStatus.CANCELLED
            seat_ids.append(t.flight_seat_id)

        if seat_ids:
            await session.execute(
                update(FlightSeat)
                .where(FlightSeat.flight_seat_id.in_(seat_ids))
                .values(status=FlightSeatStatus.AVAILABLE)
            )
            flight_res = await session.execute(
                select(FlightSeat.flight_id)
                .where(FlightSeat.flight_seat_id.in_(seat_ids))
                .distinct()
            )
            for fid in flight_res.scalars().all():
                await seat_inventory.release_seats_atomic(fid, seat_ids)

        # Invalidate booking cache
        await cache_service.delete(f"cache:booking:{booking_id}")

        return {
            "booking_id": booking.booking_id,
            "status": booking.status,
            "cancelled_tickets_count": len(tickets),
        }

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

    async def execute_and_commit_batch(
        self, batch: list[tuple[str, str, str | None]]
    ) -> list[WriteResult]:
        """Execute all events within a SINGLE batched database transaction through PgBouncer

        and pipeline result publishing + XACK.
        """
        if not batch:
            return []

        t_tx_start_perf = time.perf_counter()
        t_tx_start_wall = datetime.now(timezone.utc)
        self.on_tx_start(batch, t_tx_start_perf, t_tx_start_wall)

        results: list[WriteResult] = []
        ack_ids: list[str] = []

        async with async_session_factory() as session:
            async with session.begin():
                for msg_id, event_id, data_str in batch:
                    ack_ids.append(msg_id)
                    if not data_str:
                        continue

                    t_item_start = time.perf_counter()
                    try:
                        event_dict = json.loads(data_str)
                        event = WriteEvent.model_validate(event_dict)
                        # Savepoint per event ensures one failed booking doesn't roll back the whole batch
                        async with session.begin_nested():
                            res = await self.process_event_in_session(
                                session, event
                            )
                    except Exception as ex:
                        res = WriteResult(
                            event_id=event_id,
                            status=WriteResultStatus.ERROR,
                            error=f"Stream processing failed: {ex}",
                            status_code=400,
                        )
                    t_item_end = time.perf_counter()
                    self.on_item_processed(
                        event_id, res, t_item_start, t_item_end, msg_id
                    )
                    results.append(res)

        t_tx_complete_perf = time.perf_counter()
        t_tx_complete_wall = datetime.now(timezone.utc)
        self.on_tx_complete(batch, t_tx_complete_perf, t_tx_complete_wall)

        # Batch publish all results and XACK in a single Redis pipeline
        if results:
            redis = get_redis_client()
            pipe = redis.pipeline()
            for r in results:
                res_json = r.model_dump_json()
                pipe.set(f"write_res:{r.event_id}", res_json, ex=120)
                pipe.publish(f"write_res_chan:{r.event_id}", res_json)
            if ack_ids:
                pipe.xack(self.stream_name, self.group_name, *ack_ids)
            await pipe.execute()

        return results

    async def process_one_batch(
        self, count: int | None = None, block: int | None = None
    ) -> int:
        """Dynamic micro-batching worker ingestion & processing:

        - Inspects queue depth (unread messages backlog in Redis Stream).
        - If queue depth is low (<= queue_depth_threshold):
            Flushes immediately with minimal batch size and NO linger accumulation delay.
        - If queue depth rises (> queue_depth_threshold):
            Batches up to max_batch_size messages, accumulating incoming events
            within batch_timeout_ms, before committing in a single PostgreSQL transaction.
        """
        # 1. Inspect queue depth
        queue_depth = await self.get_queue_depth()
        is_backlog = queue_depth > self.queue_depth_threshold

        if count is not None:
            # Explicit override (e.g. caller passed a specific batch count)
            target_batch_size = count
            read_block = block if block is not None else self.block_ms
            linger_ms = 0
        elif not is_backlog:
            # Low Queue Depth: Flush immediately!
            # Target minimal batch size and do NOT linger/wait
            target_batch_size = max(
                self.min_batch_size,
                min(queue_depth if queue_depth > 0 else self.min_batch_size, self.max_batch_size),
            )
            read_block = block if block is not None else self.block_ms
            linger_ms = 0
        else:
            # Queue Depth has risen! Micro-batching active
            target_batch_size = self.max_batch_size
            read_block = block if block is not None else 0  # Non-blocking read since backlog is waiting
            linger_ms = self.batch_timeout_ms

        # 2. Ingest first chunk from Redis Stream
        t_received_perf = time.perf_counter()
        t_received_wall = datetime.now(timezone.utc)
        batch = await self._read_stream_messages(
            count=target_batch_size, block=read_block
        )

        if not batch:
            return 0

        self.on_batch_received(batch, t_received_perf, t_received_wall)

        # 3. Dynamic Micro-Batch Accumulation:
        # If queue depth is high (linger_ms > 0) and the batch is not yet full,
        # gather remaining messages up to target_batch_size until linger timeout expires.
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

        # 4. Flush the batch to PostgreSQL via PgBouncer in a single transaction
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
