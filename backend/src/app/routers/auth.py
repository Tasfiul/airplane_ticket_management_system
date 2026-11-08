from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.security import async_verify_password, create_access_token, verify_password
from app.models.customer import Customer
from app.schemas.auth import LoginRequest, TokenResponse
from app.schemas.customer import (
    CustomerCreate,
    CustomerInitiateResponse,
    CustomerResponse,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post(
    "/register",
    response_model=CustomerResponse | CustomerInitiateResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="register",
    responses={
        201: {
            "model": CustomerResponse,
            "description": "Customer registered successfully",
            "links": {
                "GetCustomer": {
                    "operationId": "get_customer",
                    "parameters": {"customer_id": "$response.body#/customer_id"},
                },
                "GetCustomerByEmail": {
                    "operationId": "get_customer_by_email",
                    "parameters": {"email": "$response.body#/email"},
                },
            },
        },
        202: {
            "model": CustomerInitiateResponse,
            "description": "Customer registration accepted for asynchronous ingestion",
        },
        400: {"description": "Bad Request"},
        409: {"description": "Customer with email already exists"},
    },
    summary="Register a new customer account (via Redis Stream)",
)
async def register(
    payload: CustomerCreate,
    response: Response,
    async_ingest: bool = Query(
        False,
        description="Whether to ingest asynchronously and return 202 Accepted immediately",
    ),
) -> CustomerResponse | CustomerInitiateResponse:
    """Registers a new customer strictly through the Redis Stream write pipeline.

    Strict architectural guarantee: no direct database writes occur over HTTP.
    - Default (async_ingest=False): synchronously waits for stream worker to commit
      and returns HTTP 201 Created with CustomerResponse.
    - Asynchronous (async_ingest=True): immediately queues write to Redis Stream and
      returns HTTP 202 Accepted with CustomerInitiateResponse in 1-2 ms.
    """
    if async_ingest:
        event_id = await write_stream_service.dispatch_async(
            action=WriteAction.CREATE_CUSTOMER,
            entity="customer",
            payload=payload.model_dump(mode="json"),
        )
        response.status_code = status.HTTP_202_ACCEPTED
        return CustomerInitiateResponse(
            event_id=event_id,
            status="PENDING",
            message="Customer registration queued for asynchronous ingestion",
        )

    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload=payload.model_dump(mode="json"),
    )
    response.status_code = status.HTTP_201_CREATED
    return CustomerResponse.model_validate(result)


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

    password_valid = False
    if customer_auth and customer_auth.get("password_hash"):
        password_valid = await async_verify_password(
            payload.password, customer_auth["password_hash"]
        )

    if not customer_auth or not password_valid:
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
