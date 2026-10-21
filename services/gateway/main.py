import os
import json
import asyncio
from typing import Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "system-events")

producer: Optional[AIOKafkaProducer] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        max_batch_size=65536,
        acks=1,
    )
    
    # Retry connection logic for cluster initialization
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

app = FastAPI(title="Ingestion Gateway API", lifespan=lifespan)

# Pydantic Schema Validation
class LogEvent(BaseModel):
    service_name: str = Field(..., examples=["auth-service"])
    event_type: str = Field(..., examples=["user_login_failed"])
    environment: list[str] = Field(default_factory=lambda: ["production"], examples=[["production"]])
    payload: str = Field(..., examples=["Invalid password attempt"])
    latency_ms: int = Field(..., ge=0, examples=[120])
    timestamp: Optional[str] = None

@app.get("/healthz", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy", "broker": KAFKA_BOOTSTRAP_SERVERS}

@app.post("/api/v1/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: LogEvent):
    if not producer:
        raise HTTPException(status_code=500, detail="Event producer unavailable")

    # Ensure UTC timestamp assignment if not supplied
    event_data = event.model_dump()
    if not event_data.get("timestamp"):
        event_data["timestamp"] = datetime.now(timezone.utc).isoformat()

    try:
        # Asynchronously send message to Redpanda topic
        await producer.send(KAFKA_TOPIC, event_data)
        return {"status": "queued", "topic": KAFKA_TOPIC}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish event: {str(e)}"
        )