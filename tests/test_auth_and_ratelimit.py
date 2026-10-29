import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, Response

# Add gateway and analytics_api directories to sys.path for direct module testing
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "services" / "gateway"))

import auth
from services.analytics_api import main as analytics_main


def test_api_key_generation_and_sha256_hashing():
    raw_key, prefix, key_hash = auth.generate_api_key()
    assert raw_key.startswith("sp_live_")
    assert prefix == raw_key[:12]
    assert len(key_hash) == 64
    assert auth.hash_api_key(raw_key) == key_hash
    # Default benchmark key hash matches SQL seed script
    assert (
        auth.hash_api_key("sp_live_default_bench_key_2026")
        == "7605d99a4e5861fa72da7af08e987f06f4b954b9968a99dfc918050151807918"
    )


def test_jwt_token_creation_and_verification():
    token = auth.create_jwt_token(
        tenant_id="tenant_fintech_01",
        scopes=["ingest:write", "analytics:read"],
        rate_limit_per_min=12000,
    )
    claims = auth.verify_jwt_token(token)
    assert claims["tenant_id"] == "tenant_fintech_01"
    assert "ingest:write" in claims["scopes"]
    assert claims["rate_limit_per_min"] == 12000
    assert claims["auth_method"] == "jwt"


def test_jwt_expired_token_rejected_401():
    expired_token = auth.create_jwt_token(
        tenant_id="tenant_expired",
        scopes=["ingest:write"],
        expires_delta=timedelta(seconds=-10),
    )
    with pytest.raises(HTTPException) as exc_info:
        auth.verify_jwt_token(expired_token)
    assert exc_info.value.status_code == 401
    assert "expired" in exc_info.value.detail.lower()


def test_resolve_api_key_l2_redis_cache_hit():
    auth._l1_auth_cache.clear()
    raw_key = "sp_live_redis_cached_key_999"
    key_hash = auth.hash_api_key(raw_key)

    mock_redis = AsyncMock()
    cached_payload = {
        "key_id": "key_999",
        "tenant_id": "tenant_redis_test",
        "tenant_name": "Redis Test Org",
        "scopes": ["ingest:write"],
        "rate_limit_per_min": 5000,
        "auth_method": "api_key",
    }
    mock_redis.get.return_value = json.dumps(cached_payload)

    with patch.object(auth, "redis_client", mock_redis):
        resolved = asyncio.run(auth.resolve_api_key(raw_key))
        assert resolved["tenant_id"] == "tenant_redis_test"
        mock_redis.get.assert_awaited_once_with(f"auth:apikey:{key_hash}")
        # Verify L1 cache was populated
        assert key_hash in auth._l1_auth_cache


def test_invalidate_api_key_cache_purges_l1_and_redis():
    raw_key = "sp_live_revoke_me_123"
    key_hash = auth.hash_api_key(raw_key)
    auth._l1_auth_cache[key_hash] = (999999999.0, {"tenant_id": "t1"})

    mock_redis = AsyncMock()
    with patch.object(auth, "redis_client", mock_redis):
        asyncio.run(auth.invalidate_api_key_cache(key_hash))
        assert key_hash not in auth._l1_auth_cache
        mock_redis.delete.assert_awaited_once_with(f"auth:apikey:{key_hash}")


def test_redis_atomic_rate_limiter_enforces_quota_and_429():
    mock_pipe = AsyncMock()
    mock_pipe.__aenter__.return_value = mock_pipe
    mock_pipe.incr = MagicMock()
    mock_pipe.expire = MagicMock()
    # Simulate 6th request when limit_per_min is 5
    mock_pipe.execute.return_value = [6, True]

    mock_redis = MagicMock()
    mock_redis.pipeline.return_value = mock_pipe

    token = auth.create_jwt_token(
        tenant_id="tenant_ratelimited",
        scopes=["ingest:write"],
        rate_limit_per_min=5,
    )

    with patch.object(auth, "redis_client", mock_redis):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                auth.authenticate_and_ratelimit(
                    x_api_key=None,
                    authorization=f"Bearer {token}",
                )
            )
        assert exc_info.value.status_code == 429
        assert "Rate limit exceeded" in exc_info.value.detail


def test_analytics_api_redis_cache_hit_and_miss():
    mock_redis = AsyncMock()
    # First call: Redis cache MISS (returns None); Second call: Redis cache HIT
    cached_summary = {
        "total_events": 100000,
        "avg_latency_ms": 23.4,
        "p99_latency_ms": 88.0,
        "p95_latency_ms": 64.2,
    }
    mock_redis.get.side_effect = [None, json.dumps(cached_summary)]

    mock_ch_result = MagicMock()
    mock_ch_result.first_row = (100000, 23.4, 88.0, 64.2)

    with patch.object(analytics_main, "redis_client", mock_redis), \
         patch.object(analytics_main, "run_clickhouse_query", return_value=mock_ch_result) as ch_mock:

        # 1st call -> MISS -> Queries ClickHouse & sets Redis cache
        resp1 = Response()
        res1 = asyncio.run(analytics_main.get_summary(resp1))
        assert resp1.headers["X-Cache"] == "MISS"
        assert res1["total_events"] == 100000
        assert ch_mock.call_count == 1
        mock_redis.setex.assert_awaited_once()

        # 2nd call -> HIT -> Served from Redis without touching ClickHouse
        resp2 = Response()
        res2 = asyncio.run(analytics_main.get_summary(resp2))
        assert resp2.headers["X-Cache"] == "HIT"
        assert res2["total_events"] == 100000
        assert ch_mock.call_count == 1
