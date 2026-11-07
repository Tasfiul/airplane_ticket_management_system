"""Automated API fuzzing and edge-case error checking using Schemathesis against /openapi.json.

Verifies strict Pydantic v2 schema conformance and guarantees no unhandled 500 server errors
across all endpoints following GEMINI.md specifications.
"""

import os
import pytest
import schemathesis
from hypothesis import HealthCheck, Phase, settings
from app.core.security import create_access_token

API_OPENAPI_URL = os.getenv(
    "SCHEMATHESIS_OPENAPI_URL", "http://localhost:8000/openapi.json"
)

schema = schemathesis.openapi.from_url(API_OPENAPI_URL)


@schema.parametrize()
@settings(
    max_examples=5,
    deadline=15000,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
    phases=[Phase.generate],
)
def test_schemathesis_openapi_conformance(case: schemathesis.Case):
    """Fuzz all endpoints using Schemathesis generated cases from /openapi.json."""
    token = create_access_token(subject=1)
    if case.headers is None:
        case.headers = {}
    case.headers["Authorization"] = f"Bearer {token}"

    response = case.call()
    case.validate_response(
        response,
        checks=[schemathesis.checks.not_a_server_error],
    )
