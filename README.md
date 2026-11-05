# Airplane Ticket Management System

High-concurrency API service and document vault for an airplane ticket management system built with FastAPI, PostgreSQL, PgBouncer, Redis, and MinIO.

## Repository Structure

```text
airplane_ticket_management_system/
├── backend/                  # FastAPI backend service, database migrations, & async workers
│   ├── src/                  # Application source code (routers, models, services, core)
│   ├── tests/                # Automated pytest suite & fuzz testing
│   ├── scripts/              # Seed scripts, benchmarks, & state reset utilities
│   ├── migrations/           # Alembic async schema migrations
│   ├── docker-compose.yml    # PostgreSQL, PgBouncer, and Redis infrastructure
│   ├── pyproject.toml        # Python project dependencies (managed by uv)
│   └── .env.example          # Environment variables template
├── GEMINI.md                 # Project technical specifications & guidelines
└── README.md                 # Root documentation
```

## Quick Start (Backend)

```bash
cd backend

# 1. Start backing services
docker compose up -d

# 2. Setup environment & install dependencies
cp .env.example .env
uv sync

# 3. Run database migrations
uv run alembic upgrade head

# 4. Run tests
uv run pytest tests/
```
