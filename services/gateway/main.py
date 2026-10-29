import os
import json
import asyncio
import secrets
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer
from sqlalchemy import select

import auth
from auth import (
    APIKey,
    Tenant,
    authenticate_and_ratelimit,
    close_auth_infrastructure,
    create_jwt_token,
    generate_api_key,
    init_auth_infrastructure,
    invalidate_api_key_cache,
    resolve_api_key,
)

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "system-events")

producer: Optional[AIOKafkaProducer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    # Initialize PostgreSQL (Async SQLAlchemy) & Redis connection pools
    await init_auth_infrastructure()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        linger_ms=5,
        max_batch_size=65536,
        acks=1,
    )

    retry_count = 0
    while retry_count < 5:
        try:
            await producer.start()
            print(f"Successfully connected to Redpanda broker at {KAFKA_BOOTSTRAP_SERVERS}")
            break
        except Exception as e:
            retry_count += 1
            print(f"Connection to Redpanda failed (Attempt {retry_count}/5): {e}")
            await asyncio.sleep(2)

    yield
    if producer:
        await producer.stop()
        print("Stopped Redpanda producer.")
    await close_auth_infrastructure()


app = FastAPI(
    title="StreamPulse Ingestion & Auth Gateway API",
    description="High-throughput ASGI event producer gateway with PostgreSQL multi-tenant API key management, JWT authentication, and Redis sliding-window rate limiting.",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class LogEvent(BaseModel):
    service_name: str = Field(..., min_length=1, examples=["auth-service"])
    event_type: str = Field(..., min_length=1, examples=["user_login_failed"])
    environment: list[str] = Field(default_factory=lambda: ["production"], examples=[["production"]])
    payload: str = Field(..., examples=["Invalid password attempt"])
    latency_ms: int = Field(..., ge=0, examples=[120])
    timestamp: Optional[str] = None


class TenantCreateRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=128, examples=["Acme Payments Ltd"])
    plan_tier: str = Field(default="pro", examples=["pro"])
    rate_limit_per_min: int = Field(default=60000, ge=10, le=5000000)


class TenantCreateResponse(BaseModel):
    tenant_id: str
    name: str
    plan_tier: str
    api_key_id: str
    raw_api_key: str
    key_prefix: str
    jwt_token: str
    rate_limit_per_min: int


class TokenExchangeResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    tenant_id: str
    scopes: List[str]
    rate_limit_per_min: int


# ---------------------------------------------------------------------------
# System & Auth Control Plane Endpoints (PostgreSQL + Redis + JWT)
# ---------------------------------------------------------------------------
@app.get("/healthz", status_code=status.HTTP_200_OK, tags=["System"])
async def health_check():
    return {
        "status": "healthy",
        "broker": KAFKA_BOOTSTRAP_SERVERS,
        "postgres_configured": auth.async_session_factory is not None,
        "redis_connected": auth.redis_client is not None,
    }


@app.post(
    "/api/v1/auth/tenants",
    response_model=TenantCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Auth & Multi-Tenant Control Plane"],
)
async def register_tenant(req: TenantCreateRequest):
    """
    Creates a new tenant organization and issues a scoped API key + HS256 JWT
    inside an ACID PostgreSQL transaction. Only the SHA-256 hash is persisted.
    """
    if auth.async_session_factory is None:
        raise HTTPException(status_code=503, detail="PostgreSQL auth database unavailable")

    tenant_id = f"tenant_{secrets.token_hex(6)}"
    key_id = f"key_{secrets.token_hex(6)}"
    raw_key, key_prefix, key_hash = generate_api_key()
    scopes_list = ["ingest:write", "analytics:read"]

    async with auth.async_session_factory() as session:
        existing = await session.scalar(select(Tenant).where(Tenant.name == req.name))
        if existing:
            raise HTTPException(status_code=409, detail=f"Tenant '{req.name}' already exists")

        new_tenant = Tenant(
            id=tenant_id,
            name=req.name,
            plan_tier=req.plan_tier,
            is_active=True,
        )
        new_key = APIKey(
            id=key_id,
            tenant_id=tenant_id,
            name=f"{req.name} Primary Key",
            key_prefix=key_prefix,
            key_hash=key_hash,
            scopes=",".join(scopes_list),
            rate_limit_per_min=req.rate_limit_per_min,
            is_active=True,
        )
        session.add(new_tenant)
        session.add(new_key)
        await session.commit()

    jwt_token = create_jwt_token(
        tenant_id=tenant_id,
        scopes=scopes_list,
        rate_limit_per_min=req.rate_limit_per_min,
    )

    return TenantCreateResponse(
        tenant_id=tenant_id,
        name=req.name,
        plan_tier=req.plan_tier,
        api_key_id=key_id,
        raw_api_key=raw_key,
        key_prefix=key_prefix,
        jwt_token=jwt_token,
        rate_limit_per_min=req.rate_limit_per_min,
    )


@app.post(
    "/api/v1/auth/token",
    response_model=TokenExchangeResponse,
    tags=["Auth & Multi-Tenant Control Plane"],
)
async def exchange_api_key_for_jwt(
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    """Exchanges a valid X-API-Key for a signed HS256 Bearer JWT."""
    auth_data = await resolve_api_key(x_api_key.strip())
    token = create_jwt_token(
        tenant_id=auth_data["tenant_id"],
        scopes=auth_data["scopes"],
        rate_limit_per_min=auth_data["rate_limit_per_min"],
    )
    return TokenExchangeResponse(
        access_token=token,
        tenant_id=auth_data["tenant_id"],
        scopes=auth_data["scopes"],
        rate_limit_per_min=auth_data["rate_limit_per_min"],
    )


@app.delete(
    "/api/v1/auth/keys/{key_id}",
    status_code=status.HTTP_200_OK,
    tags=["Auth & Multi-Tenant Control Plane"],
)
async def revoke_api_key(key_id: str):
    """Revokes an API key in PostgreSQL and immediately invalidates L1 & Redis auth caches."""
    if auth.async_session_factory is None:
        raise HTTPException(status_code=503, detail="PostgreSQL auth database unavailable")

    async with auth.async_session_factory() as session:
        key_obj = await session.get(APIKey, key_id)
        if not key_obj:
            raise HTTPException(status_code=404, detail=f"API key '{key_id}' not found")

        key_obj.is_active = False
        key_hash = key_obj.key_hash
        await session.commit()

    await invalidate_api_key_cache(key_hash)
    return {"status": "revoked", "key_id": key_id}


# Pre-allocated HTTP 202 response payload for zero-allocation hot path
_QUEUED_RESP_BYTES = f'{{"status":"queued","topic":"{KAFKA_TOPIC}"}}'.encode("utf-8")


# ---------------------------------------------------------------------------
# High-Throughput Telemetry Ingestion Endpoint
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/events",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Ingestion"],
    openapi_extra={
        "requestBody": {
            "content": {"application/json": {"schema": LogEvent.model_json_schema()}},
            "required": True,
        }
    },
)
async def ingest_event(request: Request):
    if not producer:
        raise HTTPException(status_code=500, detail="Event producer unavailable")

    # 1. Ultra-fast L1 hot-path auth & rate-limit check
    raw_key = request.headers.get("x-api-key")
    auth_header = request.headers.get("authorization")
    await authenticate_and_ratelimit(x_api_key=raw_key, authorization=auth_header)

    # 2. Single-pass Rust Pydantic v2 validation directly from raw HTTP bytes
    raw_body = await request.body()
    try:
        event = LogEvent.model_validate_json(raw_body)
    except Exception as val_err:
        raise HTTPException(status_code=422, detail=str(val_err))

    # 3. Zero-copy forward if timestamp is already present; otherwise serialize with UTC timestamp
    if event.timestamp:
        payload_bytes = raw_body
    else:
        event.timestamp = datetime.now(timezone.utc).isoformat()
        payload_bytes = event.model_dump_json().encode("utf-8")

    try:
        await producer.send(KAFKA_TOPIC, payload_bytes)
        return Response(
            content=_QUEUED_RESP_BYTES,
            status_code=202,
            media_type="application/json",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish event: {str(e)}",
        )
