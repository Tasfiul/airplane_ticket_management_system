"""Concurrency Tuning & Latency Optimization Benchmark.

Sweeps across multiple concurrency levels using the randomized fuzz query generator.
Measures latency distribution (p50, p95, p99, mean, max) and throughput (QPS).
Identifies the optimal concurrency level specifically focused on minimizing latency.
"""

import argparse
import asyncio
import json
import os
import sys
import time

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.fuzz_test import run_fuzz_benchmark


async def main():
    parser = argparse.ArgumentParser(description="Sweep concurrency levels to find optimal latency")
    parser.add_argument(
        "--levels",
        type=str,
        default="1,2,5,10,15,20,30,50,100,200",
        help="Comma-separated list of concurrency levels to test (default: 1,2,5,10,15,20,30,50,100,200)",
    )
    parser.add_argument(
        "--queries",
        type=int,
        default=1000,
        help="Number of randomized queries per concurrency level (default: 1000)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=150,
        help="Number of warmup queries before benchmark sweep (default: 150)",
    )
    args = parser.parse_args()

    levels = [int(x.strip()) for x in args.levels.split(",") if x.strip()]
    queries_per_level = args.queries

    print("=" * 80)
    print("CONCURRENCY TUNING & LATENCY OPTIMIZATION SWEEP")
    print(f"Testing Concurrency Levels: {levels}")
    print(f"Queries per Level:        {queries_per_level:,}")
    print(f"Total Sweep Workload:     {len(levels) * queries_per_level:,} complex fuzz queries")
    print("=" * 80)

    # 1. Warm-up run to eliminate cold cache bias
    if args.warmup > 0:
        print(f"\n[Warm-up] Priming PostgreSQL buffer cache with {args.warmup} queries (concurrency 10)...")
        await run_fuzz_benchmark(total_queries=args.warmup, concurrency=10, verbose=False)
        print("[Warm-up] Cache primed successfully.")

    sweep_results = []

    print("\nStarting Concurrency Sweep:")
    print("-" * 80)
    print(f"{'Concurrency':<12} | {'Throughput (QPS)':<18} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Mean (ms)':<10} | {'Wall Time':<10}")
    print("-" * 80)

    for c in levels:
        t_start = time.perf_counter()
        res = await run_fuzz_benchmark(
            total_queries=queries_per_level,
            concurrency=c,
            verbose=False,
        )
        elapsed = time.perf_counter() - t_start

        meta = res["benchmark_meta"]
        lat = res["latency_metrics_ms"]
        db_io = res["database_io_metrics"]
        res_usage = res["runtime_resources"]

        # Composite Latency Score (lower is better: 40% p50 + 60% p95)
        composite_score = round(0.4 * lat["p50_median"] + 0.6 * lat["p95"], 2)

        data_point = {
            "concurrency": c,
            "queries": queries_per_level,
            "wall_time_sec": meta["total_elapsed_sec"],
            "throughput_qps": meta["throughput_qps"],
            "p50_ms": lat["p50_median"],
            "p90_ms": lat["p90"],
            "p95_ms": lat["p95"],
            "p99_ms": lat["p99"],
            "mean_ms": lat["mean"],
            "min_ms": lat["min"],
            "max_ms": lat["max"],
            "composite_latency_score": composite_score,
            "cache_hit_ratio_percent": db_io["cache_hit_ratio_percent"],
            "postgres_ram_mb": res_usage["postgres_container_ram_mb"]["max"],
            "postgres_cpu_peak": res_usage["postgres_container_cpu_percent"]["max"],
            "pgbouncer_ram_mb": res_usage["pgbouncer_container_ram_mb"]["max"],
        }
        sweep_results.append(data_point)

        print(
            f"{c:<12} | "
            f"{data_point['throughput_qps']:<18.2f} | "
            f"{data_point['p50_ms']:<10.2f} | "
            f"{data_point['p95_ms']:<10.2f} | "
            f"{data_point['p99_ms']:<10.2f} | "
            f"{data_point['mean_ms']:<10.2f} | "
            f"{data_point['wall_time_sec']:<10.2f}s"
        )

        # Brief pause between runs for socket teardown
        await asyncio.sleep(0.5)

    print("-" * 80)

    # 2. Evaluate winners focusing on latency
    best_p50 = min(sweep_results, key=lambda x: x["p50_ms"])
    best_p95 = min(sweep_results, key=lambda x: x["p95_ms"])
    best_composite = min(sweep_results, key=lambda x: x["composite_latency_score"])
    best_throughput = max(sweep_results, key=lambda x: x["throughput_qps"])

    summary = {
        "sweep_meta": {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "tested_levels": levels,
            "queries_per_level": queries_per_level,
            "total_queries_executed": len(levels) * queries_per_level,
        },
        "best_recommendations": {
            "best_median_latency_p50": {
                "concurrency": best_p50["concurrency"],
                "p50_ms": best_p50["p50_ms"],
                "throughput_qps": best_p50["throughput_qps"],
            },
            "best_tail_latency_p95": {
                "concurrency": best_p95["concurrency"],
                "p95_ms": best_p95["p95_ms"],
                "throughput_qps": best_p95["throughput_qps"],
            },
            "best_overall_latency_score": {
                "concurrency": best_composite["concurrency"],
                "composite_score": best_composite["composite_latency_score"],
                "p50_ms": best_composite["p50_ms"],
                "p95_ms": best_composite["p95_ms"],
                "throughput_qps": best_composite["throughput_qps"],
            },
            "peak_throughput": {
                "concurrency": best_throughput["concurrency"],
                "throughput_qps": best_throughput["throughput_qps"],
                "p50_ms": best_throughput["p50_ms"],
                "p95_ms": best_throughput["p95_ms"],
            },
        },
        "data_points": sweep_results,
    }

    # Save to reports
    os.makedirs("reports", exist_ok=True)
    report_path = "reports/concurrency_tuning_results.json"
    with open(report_path, "w") as f:
        json.dump(summary, f, indent=2)

    # 3. Print Final Winner Card
    print("\n" + "=" * 80)
    print("🏆 CONCURRENCY PERFORMANCE ANALYSIS & WINNERS (FOCUS: LATENCY)")
    print("=" * 80)
    print(f"\n🥇 BEST OVERALL LATENCY WINNER: Concurrency = {best_composite['concurrency']}")
    print(f"    - Median Latency (p50):  {best_composite['p50_ms']:.2f} ms")
    print(f"    - Tail Latency (p95):    {best_composite['p95_ms']:.2f} ms")
    print(f"    - Tail Latency (p99):    {best_composite['p99_ms']:.2f} ms")
    print(f"    - Throughput:            {best_composite['throughput_qps']:.2f} QPS")
    print(f"    - Wall Time for 1k Qs:   {best_composite['wall_time_sec']:.2f} s")

    print(f"\n🔹 Best Pure Median Latency (p50): Concurrency = {best_p50['concurrency']} ({best_p50['p50_ms']:.2f} ms)")
    print(f"🔹 Best Pure Tail Latency (p95):   Concurrency = {best_p95['concurrency']} ({best_p95['p95_ms']:.2f} ms)")
    print(f"🔹 Peak Throughput Achieved:       Concurrency = {best_throughput['concurrency']} ({best_throughput['throughput_qps']:.2f} QPS)")

    print(f"\nDetailed JSON report saved to: {report_path}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
