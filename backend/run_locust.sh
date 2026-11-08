#!/usr/bin/env bash
set -e

# Change directory to backend where locust.conf and locustfile.py are located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==========================================================================="
echo "AIRPLANE TICKET MANAGEMENT SYSTEM - LOCUST LOAD TESTING"
echo "Target Host: http://localhost:80 (Nginx Reverse Proxy)"
echo "Web UI URL:  http://localhost:8089"
echo "==========================================================================="

# Check if Nginx is running on port 80
if ! curl -s -f http://localhost:80/health > /dev/null 2>&1; then
    echo "WARNING: http://localhost:80/health did not respond."
    echo "Make sure docker containers are up: 'docker compose up -d'"
fi

# Run Locust using the backend .venv with the config file
exec uv run locust --config locust.conf "$@"
