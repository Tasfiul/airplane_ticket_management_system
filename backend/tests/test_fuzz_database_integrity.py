"""Automated Pytest Suite for Redis Fuzzing Benchmark and Database Integrity Verification."""

import os
import pytest
from app.core.redis import close_redis_pool
from scripts.test_fuzz_redis_integrity import DatabaseIntegrityChecker, run_fuzz_and_verify


@pytest.fixture(autouse=True)
async def cleanup():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_fuzz_redis_and_verify_database_integrity():
    """Runs concurrent Redis Cache-Aside & Stream write fuzzing, then verifies

    all 29 relational, financial, domain, and cross-system integrity constraints.
    """
    bench_results, integrity_results, all_passed = await run_fuzz_and_verify(
        operations=60,
        concurrency=6,
        stream_workers=2,
        write_ratio=0.35,
        warmup=15,
        verbose=True,
        json_report_path="reports/pytest_integrity_results.json",
        md_report_path="reports/pytest_integrity_report.md",
    )

    # 1. Benchmark metrics assertion
    assert bench_results is not None
    meta = bench_results["benchmark_meta"]
    assert meta["total_operations"] == 60
    assert meta["success_rate_percent"] >= 80.0
    assert meta["throughput_ops_per_sec"] > 0

    # 2. Database Integrity assertions
    assert len(integrity_results) >= 28
    assert all_passed is True, "One or more database integrity checks failed!"

    # Ensure no violations in any check
    for r in integrity_results:
        assert r.passed is True, f"Integrity check failed: {r.check_id} ({r.title}) - {r.violations_count} violations"
        assert r.violations_count == 0, f"Violations found in {r.check_id}: {r.violations_count}"

    # 3. Report file existence assertions
    assert os.path.exists("reports/pytest_integrity_results.json")
    assert os.path.exists("reports/pytest_integrity_report.md")


@pytest.mark.asyncio
async def test_standalone_database_integrity_checker():
    """Verify that DatabaseIntegrityChecker can audit current database state directly."""
    checker = DatabaseIntegrityChecker()
    results = await checker.run_all_checks()

    assert len(results) >= 28
    failures = [r for r in results if not r.passed]
    assert len(failures) == 0, f"Found {len(failures)} integrity check failures: {[f.check_id for f in failures]}"
