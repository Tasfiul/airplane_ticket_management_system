from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.models.customer import Customer
from app.schemas.customer import CustomerCreate, CustomerResponse
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.write_stream import write_stream_service

router = APIRouter(prefix="/customers", tags=["Customers"])


@router.post(
    "",
    response_model=CustomerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new customer (via Redis Stream)",
)
async def create_customer(payload: CustomerCreate) -> CustomerResponse:
    """Creates a new customer by queueing the write to Redis Stream.

    No direct database writes occur over HTTP.
    """
    result = await write_stream_service.dispatch_and_wait(
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload=payload.model_dump(mode="json"),
    )
    return CustomerResponse.model_validate(result)


@router.get(
    "/{customer_id}",
    response_model=CustomerResponse,
    summary="Get customer by ID (Cache-Aside)",
)
async def get_customer(
    customer_id: int, db: AsyncSession = Depends(get_db)
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
    summary="Get customer by email (Cache-Aside)",
)
async def get_customer_by_email(
    email: str, db: AsyncSession = Depends(get_db)
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
