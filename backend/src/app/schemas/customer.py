from datetime import date, datetime
from pydantic import BaseModel, ConfigDict, EmailStr, Field


class CustomerCreate(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6)
    city: str | None = Field(default=None, max_length=100)
    postal_code: str | None = Field(default=None, max_length=20)
    division: str | None = Field(default=None, max_length=100)
    country: str | None = Field(default=None, max_length=100)
    date_of_birth: date


class CustomerResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    customer_id: int
    first_name: str
    last_name: str
    email: str
    city: str | None
    postal_code: str | None
    division: str | None
    country: str | None
    date_of_birth: date
    created_at: datetime


class CustomerUpdate(BaseModel):
    first_name: str | None = Field(default=None, min_length=1, max_length=50)
    last_name: str | None = Field(default=None, min_length=1, max_length=50)
    city: str | None = Field(default=None, max_length=100)
    postal_code: str | None = Field(default=None, max_length=20)
    division: str | None = Field(default=None, max_length=100)
    country: str | None = Field(default=None, max_length=100)
