from typing import Any
from pydantic import computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    APP_NAME: str = "Airplane Ticket Management System"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    # PostgreSQL Direct Configuration (Port 5432)
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "airplane_tickets"
    POSTGRES_SYNCHRONOUS_COMMIT: str = "on"

    # PgBouncer Configuration (Port 6432, Transaction Pooling Mode)
    USE_PGBOUNCER: bool = True
    PGBOUNCER_HOST: str = "localhost"
    PGBOUNCER_PORT: int = 6432

    # Direct database URL override (optional)
    DATABASE_URL: str | None = None

    # Redis Configuration (Cache-Aside & Stream)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None
    REDIS_MAXMEMORY: str = "512mb"
    REDIS_MAXMEMORY_POLICY: str = "allkeys-lru"
    REDIS_STREAM_WRITE_NAME: str = "stream:db_writes"
    REDIS_STREAM_CONSUMER_GROUP: str = "db_writers"
    REDIS_CACHE_DEFAULT_TTL: int = 3600
    REDIS_MAX_CONNECTIONS: int = 1000

    # Dynamic Micro-Batching Configuration (Worker Ingestion & Batching)
    STREAM_WORKER_MIN_BATCH_SIZE: int = 1
    STREAM_WORKER_MAX_BATCH_SIZE: int = 50
    STREAM_WORKER_QUEUE_DEPTH_THRESHOLD: int = 5
    STREAM_WORKER_BATCH_TIMEOUT_MS: int = 10
    STREAM_WORKER_BLOCK_MS: int = 1000
    STREAM_WORKER_FLUSH_STRATEGY: str = "dynamic"

    # Worker Process & Decoupled Architecture
    STREAM_WORKERS_COUNT: int = 4
    RUN_EMBEDDED_WORKER: bool = False
    API_PORT: int = 8000

    # JWT Security Configuration
    JWT_SECRET_KEY: str = "secret-key-airplane-ticket-management-system-super-secure-key"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    # MinIO Document Vault (S3 Compatible Storage)
    MINIO_HOST: str = "localhost"
    MINIO_PORT: int = 9000
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "tickets"
    MINIO_SECURE: bool = False
    MINIO_PUBLIC_URL: str | None = None

    @model_validator(mode="before")
    @classmethod
    def resolve_batch_aliases(cls, values: Any) -> Any:
        """Support common aliases from .env or external configurations."""
        if isinstance(values, dict):
            if "STREAM_WORKER_MIN_BATCH_SIZE" not in values:
                for alt in ("STREAM_BATCH_MIN_SIZE", "MIN_BATCH_SIZE"):
                    if alt in values:
                        values["STREAM_WORKER_MIN_BATCH_SIZE"] = values[alt]
                        break
            if "STREAM_WORKER_MAX_BATCH_SIZE" not in values:
                for alt in ("STREAM_BATCH_MAX_SIZE", "STREAM_WORKER_BATCH_SIZE", "BATCH_SIZE"):
                    if alt in values:
                        values["STREAM_WORKER_MAX_BATCH_SIZE"] = values[alt]
                        break
            if "STREAM_WORKER_QUEUE_DEPTH_THRESHOLD" not in values:
                for alt in ("STREAM_BATCH_QUEUE_DEPTH_THRESHOLD", "QUEUE_DEPTH_THRESHOLD", "STREAM_QUEUE_THRESHOLD"):
                    if alt in values:
                        values["STREAM_WORKER_QUEUE_DEPTH_THRESHOLD"] = values[alt]
                        break
            if "STREAM_WORKER_BATCH_TIMEOUT_MS" not in values:
                for alt in ("STREAM_BATCH_TIMEOUT_MS", "FLUSH_TIMEOUT_MS", "BATCH_TIMEOUT_MS"):
                    if alt in values:
                        values["STREAM_WORKER_BATCH_TIMEOUT_MS"] = values[alt]
                        break
            if "STREAM_WORKER_BLOCK_MS" not in values:
                for alt in ("STREAM_BATCH_BLOCK_MS", "REDIS_STREAM_BLOCK_MS", "BLOCK_MS"):
                    if alt in values:
                        values["STREAM_WORKER_BLOCK_MS"] = values[alt]
                        break
            if "STREAM_WORKER_FLUSH_STRATEGY" not in values:
                for alt in ("FLUSH_STRATEGY", "STREAM_FLUSH_STRATEGY"):
                    if alt in values:
                        values["STREAM_WORKER_FLUSH_STRATEGY"] = values[alt]
                        break
            if "STREAM_WORKERS_COUNT" not in values:
                for alt in ("STREAM_WORKERS", "STREAM_WORKER_CONCURRENCY", "WORKERS_COUNT"):
                    if alt in values:
                        values["STREAM_WORKERS_COUNT"] = values[alt]
                        break
            if "RUN_EMBEDDED_WORKER" not in values:
                for alt in ("EMBEDDED_WORKER", "RUN_WORKER"):
                    if alt in values:
                        values["RUN_EMBEDDED_WORKER"] = values[alt]
                        break
        return values

    @computed_field
    def redis_url(self) -> str:
        """Redis async connection URL."""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @computed_field
    def async_database_url(self) -> str:
        """PostgreSQL asyncpg connection URL routed through PgBouncer by default."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        host = self.PGBOUNCER_HOST if self.USE_PGBOUNCER else self.POSTGRES_HOST
        port = self.PGBOUNCER_PORT if self.USE_PGBOUNCER else self.POSTGRES_PORT
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{host}:{port}/{self.POSTGRES_DB}"
        )

    @computed_field
    def direct_async_database_url(self) -> str:
        """Direct connection URL to PostgreSQL (bypassing connection pooler)."""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @computed_field
    def minio_endpoint(self) -> str:
        """MinIO S3 service endpoint."""
        return f"{self.MINIO_HOST}:{self.MINIO_PORT}"


settings = Settings()
