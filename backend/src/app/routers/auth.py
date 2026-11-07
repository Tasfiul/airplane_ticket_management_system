from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.security import create_access_token, verify_password
from app.models.customer import Customer
from app.schemas.auth import LoginRequest, TokenResponse
from app.schemas.customer import CustomerResponse
from app.services.cache import cache_service

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post(
    "/login",
    response_model=TokenResponse,
    operation_id="login",
    responses={
        200: {"model": TokenResponse, "description": "Successful customer authentication"},
        401: {"description": "Incorrect email or password"},
    },
    summary="Authenticate customer and return JWT access token in JSON payload",
)
async def login(
    payload: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """Authenticates customer credentials following the Cache-Aside pattern.

    Returns a JWT access token in JSON payload for client frontend authentication.
    """
    cache_key = f"cache:customer:auth:{payload.email}"

    async def fetch_customer_auth():
        result = await db.execute(
            select(Customer).where(Customer.email == payload.email)
        )
        cust = result.scalar_one_or_none()
        if not cust:
            return None
        return {
            "customer_id": cust.customer_id,
            "first_name": cust.first_name,
            "last_name": cust.last_name,
            "email": cust.email,
            "password_hash": cust.password_hash,
            "city": cust.city,
            "postal_code": cust.postal_code,
            "division": cust.division,
            "country": cust.country,
            "date_of_birth": cust.date_of_birth.isoformat() if cust.date_of_birth else None,
            "created_at": cust.created_at.isoformat() if cust.created_at else None,
        }

    customer_auth = await cache_service.get_or_set(
        cache_key,
        fetch_customer_auth,
        ttl=600,
    )

    if not customer_auth or not verify_password(payload.password, customer_auth["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Unauthorized", "message": "Incorrect email or password"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(subject=customer_auth["customer_id"])

    # Prepare customer response object (without password_hash)
    customer_dict = dict(customer_auth)
    customer_dict.pop("password_hash", None)
    customer_resp = CustomerResponse.model_validate(customer_dict)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=3600,
        customer=customer_resp,
    )
