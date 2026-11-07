"""Tests for Nginx High-Concurrency Reverse Proxy & Load Balancer.

Verifies:
1. Pydantic Settings defaults, environment overrides, and configuration aliases.
2. Nginx configuration template syntax validity (nginx -t in Docker).
3. Reverse proxy forwarding to FastAPI backend (/health, /api/v1/*).
4. Proxy headers, rate limit headers, and Gzip compression.
5. Internal Nginx connection status monitoring (/nginx_status).
"""

import os
import subprocess
import httpx
import pytest
from app.core.config import Settings, settings


def test_nginx_settings_defaults_and_aliases():
    """Verify Nginx settings have sensible high-concurrency fallback defaults

    and support environment variable aliases.
    """
    # 1. Default fallback settings
    default_cfg = Settings()
    assert default_cfg.NGINX_PORT == 80
    assert default_cfg.NGINX_WORKER_PROCESSES == "auto"
    assert default_cfg.NGINX_WORKER_CONNECTIONS == 4096
    assert default_cfg.NGINX_WORKER_RLIMIT_NOFILE == 65536
    assert default_cfg.NGINX_CLIENT_MAX_BODY_SIZE == "20m"
    assert default_cfg.NGINX_KEEPALIVE_TIMEOUT == 65
    assert default_cfg.NGINX_PROXY_CONNECT_TIMEOUT == "10s"
    assert default_cfg.NGINX_PROXY_READ_TIMEOUT == "60s"
    assert default_cfg.NGINX_PROXY_SEND_TIMEOUT == "60s"
    assert default_cfg.NGINX_UPSTREAM_KEEPALIVE == 64
    assert default_cfg.NGINX_GZIP_ENABLED == "on"
    assert default_cfg.NGINX_GZIP_COMP_LEVEL == 5
    assert default_cfg.NGINX_ACCESS_LOG == "on"
    assert default_cfg.nginx_base_url == "http://localhost"

    # 2. Aliases support
    aliased_cfg = Settings(
        HTTP_PORT=8080,
        WORKER_CONNECTIONS=8192,
        CLIENT_MAX_BODY_SIZE="50m",
        UPSTREAM_KEEPALIVE=128,
    )
    assert aliased_cfg.NGINX_PORT == 8080
    assert aliased_cfg.NGINX_WORKER_CONNECTIONS == 8192
    assert aliased_cfg.NGINX_CLIENT_MAX_BODY_SIZE == "50m"
    assert aliased_cfg.NGINX_UPSTREAM_KEEPALIVE == 128
    assert aliased_cfg.nginx_base_url == "http://localhost:8080"


def test_nginx_config_template_validity():
    """Verify the Nginx configuration files pass syntax validation (nginx -t)

    when mounted inside an Nginx container.
    """
    cmd = [
        "docker", "run", "--rm",
        "--network", "airplane_tickets_network",
        "-e", "NGINX_PORT=80",
        "-e", "NGINX_API_HOST=api",
        "-e", "NGINX_API_PORT=8000",
        "-e", "NGINX_MINIO_HOST=minio",
        "-e", "NGINX_MINIO_PORT=9000",
        "-e", "NGINX_WORKER_PROCESSES=auto",
        "-e", "NGINX_WORKER_CONNECTIONS=4096",
        "-e", "NGINX_WORKER_RLIMIT_NOFILE=65536",
        "-e", "NGINX_CLIENT_MAX_BODY_SIZE=20m",
        "-e", "NGINX_KEEPALIVE_TIMEOUT=65",
        "-e", "NGINX_PROXY_CONNECT_TIMEOUT=10s",
        "-e", "NGINX_PROXY_READ_TIMEOUT=60s",
        "-e", "NGINX_PROXY_SEND_TIMEOUT=60s",
        "-e", "NGINX_UPSTREAM_KEEPALIVE=64",
        "-e", "NGINX_GZIP_ENABLED=on",
        "-e", "NGINX_GZIP_COMP_LEVEL=5",
        "-v", f"{os.path.abspath('nginx/nginx.conf.template')}:/etc/nginx/nginx.conf.template:ro",
        "-v", f"{os.path.abspath('nginx/templates/default.conf.template')}:/etc/nginx/templates/default.conf.template:ro",
        "-v", f"{os.path.abspath('nginx/docker-entrypoint.d/18-envsubst-nginx-conf.sh')}:/docker-entrypoint.d/18-envsubst-nginx-conf.sh:ro",
        "nginx:alpine",
        "sh", "-c",
        "/docker-entrypoint.d/18-envsubst-nginx-conf.sh && /docker-entrypoint.d/20-envsubst-on-templates.sh && nginx -t",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Nginx config validation failed: {res.stderr}\n{res.stdout}"
    assert "syntax is ok" in res.stderr or "syntax is ok" in res.stdout
    assert "test is successful" in res.stderr or "test is successful" in res.stdout


@pytest.mark.asyncio
async def test_nginx_reverse_proxy_health():
    """Verify that requests to Nginx /health proxy directly to the FastAPI

    backend and return a healthy status.
    """
    nginx_url = f"http://127.0.0.1:{settings.NGINX_PORT}/health"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(nginx_url, timeout=5.0)
        except httpx.ConnectError:
            pytest.skip(f"Nginx is not running on {nginx_url}")

        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["database"]["status"] == "UP"
        assert data["redis"]["status"] == "UP"
        assert data["minio"]["status"] == "UP"
        # Verify Server header comes from Nginx
        server_header = resp.headers.get("server", "").lower()
        assert "nginx" in server_header, f"Expected Nginx in Server header, got: {server_header}"


@pytest.mark.asyncio
async def test_nginx_api_routing_and_rate_limit_headers():
    """Verify API endpoint routing through Nginx preserves rate limit headers and JSON payloads."""
    nginx_url = f"http://127.0.0.1:{settings.NGINX_PORT}/api/v1/flights/airports"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(nginx_url, timeout=5.0)
        except httpx.ConnectError:
            pytest.skip(f"Nginx is not running on {nginx_url}")

        assert resp.status_code == 200, resp.text
        airports = resp.json()
        assert isinstance(airports, list)
        assert len(airports) >= 10
        # Verify RFC rate limit headers passed through
        assert "x-ratelimit-limit" in resp.headers
        assert "x-ratelimit-remaining" in resp.headers


@pytest.mark.asyncio
async def test_nginx_gzip_compression():
    """Verify Nginx Gzip compression is active when client sends Accept-Encoding: gzip."""
    nginx_url = f"http://127.0.0.1:{settings.NGINX_PORT}/api/v1/flights/airports"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                nginx_url,
                headers={"Accept-Encoding": "gzip"},
                timeout=5.0,
            )
        except httpx.ConnectError:
            pytest.skip(f"Nginx is not running on {nginx_url}")

        assert resp.status_code == 200
        # When compressed, Content-Encoding is gzip
        assert resp.headers.get("content-encoding") == "gzip"


@pytest.mark.asyncio
async def test_nginx_stub_status_monitoring():
    """Verify Nginx /nginx_status stub_status endpoint reports active connections."""
    status_url = f"http://127.0.0.1:{settings.NGINX_PORT}/nginx_status"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(status_url, timeout=5.0)
        except httpx.ConnectError:
            pytest.skip(f"Nginx is not running on {status_url}")

        assert resp.status_code == 200, resp.text
        text = resp.text
        assert "Active connections:" in text
        assert "server accepts handled requests" in text
