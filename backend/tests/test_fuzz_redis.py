import os
import pytest
from app.core.redis import close_redis_pool
from scripts.fuzz_test_redis import run_redis_fuzz_benchmark


@pytest.fixture(autouse=True)
async def cleanup():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_concurrent_fuzz_benchmark_reads_and_writes():
    """Execute a fuzzing benchmark with concurrent reads (Cache-Aside) and writes

    (Redis Stream via PgBouncer), verifying metrics collection and reporting.
    """
    total_ops = 60
    concurrency = 8
    stream_workers = 2
    write_ratio = 0.35

    results = await run_redis_fuzz_benchmark(
        total_operations=total_ops,
        concurrency=concurrency,
        num_stream_workers=stream_workers,
        write_ratio=write_ratio,
        warmup=20,
        verbose=True,
    )

    meta = results["benchmark_meta"]
    assert meta["total_operations"] == total_ops
    assert meta["success_rate_percent"] >= 90.0
    assert meta["throughput_ops_per_sec"] > 0

    reads = results["reads_summary"]
    writes = results["writes_summary"]
    assert reads["count"] > 0
    assert writes["count"] > 0

    # Read latency should be low due to cache-aside
    assert reads["latency_ms"]["mean"] > 0
    # Write latency through Redis Stream and PgBouncer should be recorded
    assert writes["latency_ms"]["mean"] > 0

    # Ensure reports were generated
    assert os.path.exists("reports/redis_fuzz_results.json")
    assert os.path.exists("reports/redis_fuzz_report.md")
