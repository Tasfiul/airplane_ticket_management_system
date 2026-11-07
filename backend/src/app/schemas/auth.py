from pydantic import BaseModel, EmailStr, Field
from app.schemas.customer import CustomerResponse


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 3600
    customer: CustomerResponse
