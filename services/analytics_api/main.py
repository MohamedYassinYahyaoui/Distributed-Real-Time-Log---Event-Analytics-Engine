import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Response
import clickhouse_connect
import redis.asyncio as aioredis

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment configuration
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "5"))

redis_client: Optional[aioredis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    try:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        logging.info(f"Analytics API connected to Redis cache at {REDIS_URL}")
    except Exception as e:
        logging.warning(f"Redis cache unavailable ({e}); serving directly from ClickHouse.")
        redis_client = None
    yield
    if redis_client:
        await redis_client.aclose()


app = FastAPI(
    title="StreamPulse Analytics API",
    description="Real-time OLAP analytics engine exposing system metrics and latency percentiles (P95/P99) from ClickHouse with Redis read-through query caching.",
    version="2.0.0",
    lifespan=lifespan,
)


def run_clickhouse_query(query: str) -> Any:
    """Synchronous helper to execute ClickHouse queries in a worker thread."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    return client.query(query)


@app.get("/health", tags=["System"])
async def health_check():
    """Service health check endpoint."""
    return {
        "status": "healthy",
        "redis_cache": redis_client is not None,
    }


@app.get("/api/v1/analytics/summary", tags=["Analytics"])
async def get_summary(response: Response) -> Dict[str, Any]:
    """Retrieve aggregate metrics including event counts and latency percentiles (P95, P99)."""
    cache_key = "analytics:summary:v1"

    if redis_client is not None:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                response.headers["X-Cache"] = "HIT"
                return json.loads(cached)
        except Exception as e:
            logging.debug(f"Redis cache read warning: {e}")

    response.headers["X-Cache"] = "MISS"
    query = """
    SELECT 
        count() as total_events,
        avg(latency_ms) as avg_latency_ms,
        quantile(0.99)(latency_ms) as p99_latency_ms,
        quantile(0.95)(latency_ms) as p95_latency_ms
    FROM default.system_events
    """
    try:
        result = await asyncio.to_thread(run_clickhouse_query, query)
        row = result.first_row

        if not row or row[0] == 0:
            payload = {
                "total_events": 0,
                "avg_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
            }
        else:
            payload = {
                "total_events": row[0],
                "avg_latency_ms": round(row[1], 2) if row[1] is not None else 0.0,
                "p99_latency_ms": round(row[2], 2) if row[2] is not None else 0.0,
                "p95_latency_ms": round(row[3], 2) if row[3] is not None else 0.0,
            }

        if redis_client is not None and payload["total_events"] > 0:
            try:
                await redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload))
            except Exception:
                pass

        return payload
    except Exception as e:
        logging.error(f"Error querying summary analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.get("/api/v1/analytics/services", tags=["Analytics"])
async def get_service_metrics(response: Response) -> List[Dict[str, Any]]:
    """Retrieve breakdown of event volume and latency distribution grouped by service."""
    cache_key = "analytics:services:v1"

    if redis_client is not None:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                response.headers["X-Cache"] = "HIT"
                return json.loads(cached)
        except Exception as e:
            logging.debug(f"Redis cache read warning: {e}")

    response.headers["X-Cache"] = "MISS"
    query = """
    SELECT 
        service_name,
        count() as event_count,
        avg(latency_ms) as avg_latency,
        quantile(0.99)(latency_ms) as p99_latency
    FROM default.system_events
    GROUP BY service_name
    ORDER BY event_count DESC
    """
    try:
        result = await asyncio.to_thread(run_clickhouse_query, query)

        payload = [
            {
                "service_name": str(row[0]),
                "event_count": row[1],
                "avg_latency_ms": round(row[2], 2) if row[2] is not None else 0.0,
                "p99_latency_ms": round(row[3], 2) if row[3] is not None else 0.0,
            }
            for row in result.result_rows
        ]

        if redis_client is not None and payload:
            try:
                await redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload))
            except Exception:
                pass

        return payload
    except Exception as e:
        logging.error(f"Error querying service analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
