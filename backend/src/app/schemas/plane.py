from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class PlaneCreate(BaseModel):
    tail_number: str = Field(..., min_length=3, max_length=20, examples=["N999AA"])
    model: str = Field(..., min_length=2, max_length=50, examples=["Boeing 737-800"])
    total_seats: int = Field(default=200, ge=10, le=800, examples=[200])


class PlaneResponse(BaseModel):
    plane_id: int
    tail_number: str
    model: str
    total_seats: int

    model_config = ConfigDict(from_attributes=True)


class PlaneListResponse(BaseModel):
    planes: list[PlaneResponse]
    total: int
    page: int
    page_size: int


class AdminMetricsResponse(BaseModel):
    status: str
    timestamp: str
    redis: dict[str, Any]
    database: dict[str, Any]
    stream_worker: dict[str, Any]
    minio: dict[str, Any]
