import asyncio
from datetime import datetime, timezone
from typing import Any
import uuid
from fastapi import HTTPException
import orjson
from app.core.config import settings
from app.core.redis import get_redis_client
from app.schemas.write_event import (
    WriteAction,
    WriteEvent,
    WriteResult,
    WriteResultStatus,
)


class WriteStreamService:
    """Dispatches database write operations exclusively through Redis Streams.

    Enforces the architectural constraint: no write operation touches the database
    directly over HTTP.
    """

    def __init__(self, stream_name: str = settings.REDIS_STREAM_WRITE_NAME):
        self.stream_name = stream_name

    async def dispatch_and_wait(
        self,
        action: WriteAction,
        entity: str,
        payload: dict[str, Any],
        timeout: float = 10.0,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Dispatch a write event to Redis Stream and wait for the consumer worker

        to complete the database transaction via PgBouncer.
        """
        redis = get_redis_client()
        if event_id is None:
            event_id = str(uuid.uuid4())
        event = WriteEvent(
            event_id=event_id,
            action=action,
            entity=entity,
            payload=payload,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        result_key = f"write_res:{event_id}"
        channel_name = f"write_res_chan:{event_id}"

        pubsub = redis.pubsub()
        await pubsub.subscribe(channel_name)

        try:
            # 1. Dispatch event to Redis Stream
            stream_payload = {"event_id": event_id, "data": event.model_dump_json()}
            await redis.xadd(self.stream_name, stream_payload)

            # 2. Check if already processed (fast-path)
            cached_res = await redis.get(result_key)
            if cached_res:
                return self._parse_result(cached_res)

            # 3. Wait for PubSub notification or timeout
            start_time = asyncio.get_event_loop().time()
            while True:
                remaining_time = timeout - (
                    asyncio.get_event_loop().time() - start_time
                )
                if remaining_time <= 0:
                    break

                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=0.1
                        ),
                        timeout=min(remaining_time, 0.5),
                    )
                    if message and message["type"] == "message":
                        data_val = message.get("data")
                        if data_val and data_val != "done":
                            return self._parse_result(data_val)
                        break
                except asyncio.TimeoutError:
                    pass

                # Double-check result key in case pubsub event was missed
                cached_res = await redis.get(result_key)
                if cached_res:
                    return self._parse_result(cached_res)

            # Final check after loop
            cached_res = await redis.get(result_key)
            if cached_res:
                return self._parse_result(cached_res)

            raise HTTPException(
                status_code=504,
                detail={
                    "error": "Gateway Timeout",
                    "message": f"Write stream processing timed out after {timeout}s for event {event_id}",
                    "event_id": event_id,
                },
            )

        finally:
            await pubsub.unsubscribe(channel_name)
            await pubsub.aclose()

    async def dispatch_async(
        self,
        action: WriteAction,
        entity: str,
        payload: dict[str, Any],
        event_id: str | None = None,
    ) -> str:
        """Dispatch a write event to Redis Stream without waiting (Fire & Check later).

        Returns the event_id.
        """
        redis = get_redis_client()
        if event_id is None:
            event_id = str(uuid.uuid4())
        event = WriteEvent(
            event_id=event_id,
            action=action,
            entity=entity,
            payload=payload,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        stream_payload = {"event_id": event_id, "data": event.model_dump_json()}
        await redis.xadd(self.stream_name, stream_payload)
        return event_id

    def _parse_result(self, raw_result: str | bytes) -> dict[str, Any]:
        result_dict = orjson.loads(raw_result)
        result = WriteResult.model_validate(result_dict)
        if result.status == WriteResultStatus.ERROR:
            raise HTTPException(
                status_code=result.status_code,
                detail={
                    "error": "Write Operation Failed",
                    "message": result.error,
                    "event_id": result.event_id,
                },
            )
        return result.data or {}


write_stream_service = WriteStreamService()
