"""Standalone Automated Schemathesis Runner for OpenAPI Fuzzing.

Executes Schemathesis fuzzing against /openapi.json using realistic JWT bearer credentials,
monitoring for 500 server errors, invalid status codes, and schema mismatches.
"""

import argparse
import sys
import subprocess
from app.core.security import create_access_token


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Schemathesis fuzzing against OpenAPI schema.")
    parser.add_argument(
        "--openapi-url",
        default="http://localhost:8000/openapi.json",
        help="Target OpenAPI specification URL (default: http://localhost:8000/openapi.json)",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=10,
        help="Maximum generated hypothesis examples per operation (default: 10)",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=15,
        help="HTTP request timeout in seconds (default: 15)",
    )
    parser.add_argument(
        "--auth-customer-id",
        type=int,
        default=1,
        help="Customer ID used to mint JWT Bearer access token (default: 1)",
    )
    parser.add_argument(
        "--checks",
        default="not_a_server_error",
        help="Comma-separated schemathesis checks to enforce (default: not_a_server_error)",
    )
    parser.add_argument(
        "--warnings",
        default=None,
        help="Control warning display: 'off' to disable all, or comma-separated list of warning types",
    )
    parser.add_argument(
        "--rate-limit",
        default=None,
        help="Specify rate limit for requests (e.g. '100/m' or 'auto' to respect Retry-After)",
    )
    args = parser.parse_args()

    token = create_access_token(subject=args.auth_customer_id)
    print("=" * 70)
    print(f"Running Schemathesis Fuzzing against: {args.openapi_url}")
    print(f"Max Examples per Operation: {args.max_examples}")
    print(f"Request Timeout: {args.request_timeout}s")
    print(f"Checks: {args.checks}")
    if args.warnings:
        print(f"Warnings: {args.warnings}")
    if args.rate_limit:
        print(f"Rate Limit: {args.rate_limit}")
    print("=" * 70)

    cmd = [
        "schemathesis", "run", args.openapi_url,
        "--max-examples", str(args.max_examples),
        "--checks", args.checks,
        "--request-timeout", str(args.request_timeout),
        "-H", f"Authorization: Bearer {token}",
    ]
    if args.warnings:
        cmd.extend(["--warnings", args.warnings])
    if args.rate_limit:
        cmd.extend(["--rate-limit", args.rate_limit])

    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
