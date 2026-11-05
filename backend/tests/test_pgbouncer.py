import asyncio
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from app.core.config import settings


@pytest.mark.asyncio
async def test_pgbouncer_transaction_pooling():
    """Verify that multiple concurrent async transactions work through PgBouncer in transaction pooling mode."""
    # Use NullPool for the test runner to avoid cross-event-loop connection sharing
    engine = create_async_engine(
        settings.async_database_url,
        poolclass=NullPool,
        connect_args={"statement_cache_size": 0},
    )
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def worker(idx: int):
        async with session_factory() as session:
            result = await session.execute(
                text("SELECT CAST(:num AS INTEGER) AS n"),
                {"num": idx},
            )
            val = result.scalar()
            assert val == idx

            table_check = await session.execute(text("SELECT count(*) FROM flight"))
            count = table_check.scalar()
            assert count >= 0
            return val

    # Run 50 concurrent transactions simultaneously through PgBouncer
    tasks = [worker(i) for i in range(50)]
    results = await asyncio.gather(*tasks)
    assert len(results) == 50
    assert results == list(range(50))

    await engine.dispose()
