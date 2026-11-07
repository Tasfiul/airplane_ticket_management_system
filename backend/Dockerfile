# Multi-stage Dockerfile for Airplane Ticket Management System
# Stage 1: Build virtual environment and compile wheels
FROM python:3.12-slim-bookworm AS builder

WORKDIR /build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Stage 2: Final minimal runtime image
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src:/app"

# Install runtime dependencies only (libpq for asyncpg/psycopg, curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create unprivileged application user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash -m appuser

# Copy application source code and migrations
COPY src/ /app/src/
COPY pyproject.toml README.md /app/
COPY alembic.ini /app/
COPY migrations/ /app/migrations/

# Ensure proper permissions
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

# Default command starts the FastAPI web server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
