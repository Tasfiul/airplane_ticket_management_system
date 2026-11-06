from pydantic import computed_field
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


settings = Settings()
