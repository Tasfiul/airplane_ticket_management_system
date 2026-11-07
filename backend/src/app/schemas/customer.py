from datetime import date, datetime
from typing import Any
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class CustomerCreate(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=50, pattern=r"^[^\x00]+$", examples=["John"])
    last_name: str = Field(..., min_length=1, max_length=50, pattern=r"^[^\x00]+$", examples=["Doe"])
    email: EmailStr = Field(..., examples=["john.doe@example.com"])
    password: str = Field(..., min_length=6, max_length=72, pattern=r"^[^\x00]+$", examples=["SecurePass123!"])
    city: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
    postal_code: str | None = Field(default=None, max_length=20, pattern=r"^[^\x00]*$")
    division: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
    country: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
    date_of_birth: date = Field(..., examples=["1990-01-01"])


class CustomerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    customer_id: int = Field(..., ge=1, le=2_147_483_647)
    first_name: str
    last_name: str
    email: str
    city: str | None
    postal_code: str | None
    division: str | None
    country: str | None
    date_of_birth: date
    created_at: datetime | None = None


class CustomerUpdate(BaseModel):
    first_name: str | None = Field(default=None, min_length=1, max_length=50, pattern=r"^[^\x00]+$")
    last_name: str | None = Field(default=None, min_length=1, max_length=50, pattern=r"^[^\x00]+$")
    city: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
    postal_code: str | None = Field(default=None, max_length=20, pattern=r"^[^\x00]*$")
    division: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
    country: str | None = Field(default=None, max_length=100, pattern=r"^[^\x00]*$")
