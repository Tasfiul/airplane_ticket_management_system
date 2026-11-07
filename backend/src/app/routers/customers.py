from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.core.security import get_current_customer
from app.models.booking import Booking
from app.models.customer import Customer
from app.schemas.booking import BookingResponse
from app.schemas.customer import (
    CustomerCreate,
    CustomerInitiateResponse,
    CustomerResponse,
    CustomerUpdate,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/customers", tags=["Customers"])


@router.post(
    "",
    response_model=CustomerResponse | CustomerInitiateResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_customer",
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
    summary="Register a new customer (via Redis Stream)",
)
async def create_customer(
    payload: CustomerCreate,
    response: Response,
    async_ingest: bool = Query(
        False,
        description="Whether to ingest asynchronously and return 202 Accepted immediately",
    ),
) -> CustomerResponse | CustomerInitiateResponse:
    """Creates a new customer by queueing the write to Redis Stream.

    No direct database writes occur over HTTP.
    When async_ingest=True, ingests into Redis Stream and returns 202 Accepted immediately.
    When async_ingest=False (default), synchronously waits for DB commit and returns 201 Created.
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


@router.get(
    "/me",
    response_model=CustomerResponse,
    operation_id="get_current_customer_profile",
    summary="Get current logged-in customer profile (Cache-Aside)",
)
async def get_current_customer_profile(
    current_customer: CustomerResponse = Depends(get_current_customer),
) -> CustomerResponse:
    """Returns the authenticated customer's profile directly from Redis Cache-Aside."""
    return current_customer


@router.patch(
    "/me",
    response_model=CustomerResponse,
    operation_id="update_current_customer_profile",
    summary="Update current customer profile (via Redis Stream)",
)
async def update_current_customer_profile(
    payload: CustomerUpdate,
    current_customer: CustomerResponse = Depends(get_current_customer),
) -> CustomerResponse:
    """Updates current customer profile details strictly through Redis Stream.

    No direct database writes occur over HTTP.
    """
    update_data = payload.model_dump(exclude_unset=True)
    if not update_data:
        return current_customer

    update_data["customer_id"] = current_customer.customer_id
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.UPDATE_CUSTOMER,
        entity="customer",
        payload=update_data,
    )
    return CustomerResponse.model_validate(result)


@router.get(
    "/me/bookings",
    response_model=list[BookingResponse],
    operation_id="get_my_bookings",
    summary="Get booking history for the logged-in customer (Cache-Aside)",
)
async def get_my_bookings(
    page: int = Query(1, ge=1, le=2_147_483_647, description="Page number (1-based)"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page (max 100)"),
    current_customer: CustomerResponse = Depends(get_current_customer),
    db: AsyncSession = Depends(get_db),
) -> list[BookingResponse]:
    """Fetch customer's booking history following the Cache-Aside pattern.

    Uses eager loading (selectinload) to eliminate N+1 queries.
    """
    cache_key = f"cache:customer:{current_customer.customer_id}:bookings:p{page}:s{page_size}"

    async def fetch_from_db():
        stmt = (
            select(Booking)
            .where(Booking.customer_id == current_customer.customer_id)
            .order_by(Booking.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .options(selectinload(Booking.tickets))
        )
        result = await db.execute(stmt)
        bookings = result.scalars().all()
        return [
            BookingResponse.model_validate(b).model_dump(mode="json")
            for b in bookings
        ]

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=300,
    )
    return [BookingResponse.model_validate(b) for b in (cached_data or [])]


@router.get(
    "/{customer_id}",
    response_model=CustomerResponse,
    operation_id="get_customer",
    responses={
        200: {"model": CustomerResponse, "description": "Customer details"},
        404: {"description": "Customer not found"},
    },
    summary="Get customer by ID (Cache-Aside)",
)
async def get_customer(
    customer_id: int = Path(..., ge=1, le=2_147_483_647, description="Customer ID", examples=[1]),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """Fetch customer details following the cache-aside pattern."""
    cache_key = f"cache:customer:{customer_id}"

    async def fetch_from_db():
        result = await db.execute(
            select(Customer).where(Customer.customer_id == customer_id)
        )
        customer = result.scalar_one_or_none()
        if not customer:
            return None
        return CustomerResponse.model_validate(customer).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=3600,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Customer {customer_id} not found"},
        )

    return CustomerResponse.model_validate(cached_data)


@router.get(
    "/by-email/{email}",
    response_model=CustomerResponse,
    operation_id="get_customer_by_email",
    responses={
        200: {"model": CustomerResponse, "description": "Customer details"},
        404: {"description": "Customer not found"},
    },
    summary="Get customer by email (Cache-Aside)",
)
async def get_customer_by_email(
    email: str = Path(..., max_length=128, pattern=r"^[^\x00]+$", description="Customer email address"),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """Fetch customer by email following the cache-aside pattern."""
    cache_key = f"cache:customer:email:{email}"

    async def fetch_from_db():
        result = await db.execute(select(Customer).where(Customer.email == email))
        customer = result.scalar_one_or_none()
        if not customer:
            return None
        return CustomerResponse.model_validate(customer).model_dump(mode="json")

    cached_data = await cache_service.get_or_set(
        cache_key,
        fetch_from_db,
        ttl=3600,
    )

    if not cached_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not Found", "message": f"Customer with email {email} not found"},
        )

    return CustomerResponse.model_validate(cached_data)
