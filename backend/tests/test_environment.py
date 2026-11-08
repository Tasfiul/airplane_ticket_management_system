import io
import pytest
import bcrypt
import jwt
from fastapi import FastAPI
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import create_async_engine
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import redis.asyncio as aioredis
from minio import Minio


@pytest.mark.asyncio
async def test_fastapi_and_pydantic():
    app = FastAPI(title="Test App")

    class Ticket(BaseModel):
        id: int
        passenger_email: EmailStr
        seat: str

    t = Ticket(id=1, passenger_email="user@example.com", seat="12A")
    assert t.passenger_email == "user@example.com"
    assert app.title == "Test App"


@pytest.mark.asyncio
async def test_sqlalchemy_asyncpg_engine_setup():
    engine = create_async_engine(
        "postgresql+asyncpg://user:pass@localhost:5432/testdb",
        connect_args={"statement_cache_size": 0},
    )
    assert engine is not None
    await engine.dispose()


def test_pdf_generation_in_memory():
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    c.drawString(100, 750, "Ticket PDF Test")
    c.save()
    buffer.seek(0)
    data = buffer.read()
    assert len(data) > 0
    assert data.startswith(b"%PDF")


def test_auth_bcrypt_jwt():
    pw = b"secure_password"
    hashed = bcrypt.hashpw(pw, bcrypt.gensalt())
    assert bcrypt.checkpw(pw, hashed)

    secret = "a" * 32
    token = jwt.encode({"sub": "admin"}, secret, algorithm="HS256")
    payload = jwt.decode(token, secret, algorithms=["HS256"])
    assert payload["sub"] == "admin"


def test_clients_init():
    minio_client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    assert minio_client is not None

    redis_client = aioredis.from_url("redis://localhost:6379/0")
    assert redis_client is not None


def test_bcrypt_rounds_settings_and_aliases():
    """Verify BCRYPT_ROUNDS setting and aliases."""
    from app.core.config import Settings

    default_s = Settings(_env_file=None)
    assert default_s.BCRYPT_ROUNDS == 12

    s_alias1 = Settings(_env_file=None, PASSWORD_BCRYPT_ROUNDS=6)
    assert s_alias1.BCRYPT_ROUNDS == 6

    s_alias2 = Settings(_env_file=None, HASH_ROUNDS=4)
    assert s_alias2.BCRYPT_ROUNDS == 4


@pytest.mark.asyncio
async def test_async_password_hashing_and_verification():
    """Verify async_verify_password and async_hash_password run in worker threads."""
    from app.core.security import (
        async_hash_password,
        async_verify_password,
        hash_password,
        verify_password,
    )

    pw = "SuperSecretPassword123!"
    hashed = await async_hash_password(pw)
    assert hashed.startswith("$2b$")

    # Verify matching password
    assert await async_verify_password(pw, hashed) is True
    assert verify_password(pw, hashed) is True

    # Verify wrong password
    assert await async_verify_password("WrongPassword!", hashed) is False
    assert verify_password("WrongPassword!", hashed) is False
