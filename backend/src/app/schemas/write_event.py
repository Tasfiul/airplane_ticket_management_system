from datetime import datetime, timezone
from enum import Enum
from typing import Any
import uuid
from pydantic import BaseModel, Field


class WriteAction(str, Enum):
    CREATE_CUSTOMER = "CREATE_CUSTOMER"
    UPDATE_CUSTOMER = "UPDATE_CUSTOMER"
    CREATE_FLIGHT = "CREATE_FLIGHT"
    UPDATE_FLIGHT_STATUS = "UPDATE_FLIGHT_STATUS"
    CREATE_BOOKING = "CREATE_BOOKING"
    CANCEL_BOOKING = "CANCEL_BOOKING"
    CONFIRM_BOOKING = "CONFIRM_BOOKING"
    CREATE_PLANE = "CREATE_PLANE"


class WriteEvent(BaseModel):
    """Event payload serialized into the Redis Stream for asynchronous/decoupled DB writes."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    action: WriteAction
    entity: str
    payload: dict[str, Any]
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class WriteResultStatus(str, Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class WriteResult(BaseModel):
    """Result of a Redis Stream DB write operation dispatched to the worker."""

    event_id: str
    status: WriteResultStatus
    data: dict[str, Any] | None = None
    error: str | None = None
    status_code: int = 200
