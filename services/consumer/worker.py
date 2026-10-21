import os
import json
import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import List, Dict, Any
from aiokafka import AIOKafkaConsumer
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "system-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "stream-pulse-consumer-group")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# Micro-batching tuning thresholds
BATCH_SIZE = 10000
BATCH_TIMEOUT_SECONDS = 1.0


def init_clickhouse_tables(client):
    """Ensures primary tables and DLQ tables exist prior to batch processing."""
    client.command("""
    CREATE TABLE IF NOT EXISTS system_events (
        timestamp DateTime64(3, 'UTC'),
        service_name String,
        event_type String,
        environment String,
        payload String,
        latency_ms Int32
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, service_name, event_type);
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS system_events_dlq (
        timestamp DateTime64(3, 'UTC') DEFAULT now(),
        raw_payload String,
        error_message String
    ) ENGINE = MergeTree()
    ORDER BY timestamp;
    """)
    logging.info("ClickHouse schema verified successfully.")


def _sync_insert(client, table: str, data: list, column_names: list):
    """Synchronous insertion call meant to execute within a thread pool."""
    client.insert(table=table, data=data, column_names=column_names)


async def flush_to_clickhouse(client, batch: List[Dict[str, Any]]) -> bool:
    if not batch:
        return True

    rows = []
    for event in batch:
        raw_ts = event.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                clean_ts = raw_ts.replace('Z', '+00:00')
                ts_obj = datetime.fromisoformat(clean_ts)
            except ValueError:
                ts_obj = datetime.now(timezone.utc)
        elif isinstance(raw_ts, datetime):
            ts_obj = raw_ts
        else:
            ts_obj = datetime.now(timezone.utc)

        raw_payload = event.get("payload", "")
        payload_str = json.dumps(raw_payload) if isinstance(raw_payload, (dict, list)) else str(raw_payload)

        rows.append((
            ts_obj,
            str(event.get("service_name", "unknown")),
            str(event.get("event_type", "unknown")),
            str(event.get("environment", ["production"])),
            payload_str,
            int(event.get("latency_ms", 0))
        ))

    try:
        # Offload sync ClickHouse HTTP write to thread pool to avoid blocking asyncio loop
        await asyncio.to_thread(
            _sync_insert,
            client,
            'system_events',
            rows,
            ['timestamp', 'service_name', 'event_type', 'environment', 'payload', 'latency_ms']
        )
        logging.info(f"Persisted micro-batch of {len(batch)} records to ClickHouse.")
        return True
    except Exception as e:
        logging.error(f"Failed to insert batch into ClickHouse: {e}")
        try:
            dlq_rows = [(json.dumps(event), str(e)) for event in batch]
            await asyncio.to_thread(
                _sync_insert,
                client,
                'system_events_dlq',
                dlq_rows,
                ['raw_payload', 'error_message']
            )
            logging.warning(f"Routed {len(batch)} failed records to DLQ table.")
            return True
        except Exception as dlq_err:
            logging.critical(f"DLQ insertion failed: {dlq_err}")
            return False


async def flush_to_dlq(client, invalid_batch: List[tuple[str, str]]) -> bool:
    if not invalid_batch:
        return True

    try:
        await asyncio.to_thread(
            _sync_insert,
            client,
            'system_events_dlq',
            invalid_batch,
            ['raw_payload', 'error_message']
        )
        logging.warning(f"Routed {len(invalid_batch)} invalid records to DLQ.")
        return True
    except Exception as err:
        logging.critical(f"DLQ insertion failed: {err}")
        return False


def parse_event(raw_value: bytes) -> tuple[Dict[str, Any] | None, str | None]:
    try:
        event = json.loads(raw_value.decode('utf-8'))
    except Exception as err:
        return None, f"invalid JSON: {err}"

    if not isinstance(event, dict):
        return None, "event must be a JSON object"
    if not isinstance(event.get("service_name"), str) or not event["service_name"]:
        return None, "service_name must be a non-empty string"
    if not isinstance(event.get("event_type"), str) or not event["event_type"]:
        return None, "event_type must be a non-empty string"
    if not isinstance(event.get("environment"), list):
        return None, "environment must be a list"
    if not isinstance(event.get("latency_ms"), int) or event["latency_ms"] < 0:
        return None, "latency_ms must be a non-negative integer"

    return event, None


async def commit_offsets(consumer: AIOKafkaConsumer):
    """Commits current consumer offsets after batch persistence is confirmed."""
    try:
        await consumer.commit()
    except Exception as e:
        logging.error(f"Failed to commit offsets to broker: {e}")


async def main():
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    logging.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    
    # Run DDL queries safely at startup
    await asyncio.to_thread(init_clickhouse_tables, ch_client)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )

    retry_count = 0
    while retry_count < 10:
        try:
            await consumer.start()
            logging.info(f"Consumer listening on Redpanda topic '{KAFKA_TOPIC}'...")
            break
        except Exception as e:
            retry_count += 1
            logging.warning(f"Waiting for Redpanda broker ({retry_count}/10): {e}")
            await asyncio.sleep(2)

    if retry_count >= 10:
        logging.critical("Could not connect to Redpanda broker after 10 attempts. Exiting.")
        return

    batch: List[Dict[str, Any]] = []
    invalid_batch: List[tuple[str, str]] = []
    last_flush_time = asyncio.get_event_loop().time()
    is_running = True

    def stop_signal_handler():
        nonlocal is_running
        logging.info("Termination signal received. Shutting down consumer loop...")
        is_running = False

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal_handler)
        except NotImplementedError:
            # Signal handling behavior on non-POSIX / Windows environments
            pass

    try:
        while is_running:
            try:
                msg_map = await consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE)
                for tp, messages in msg_map.items():
                    for msg in messages:
                        raw_value = msg.value or b""
                        event, error = parse_event(raw_value)
                        if event is not None:
                            batch.append(event)
                        else:
                            invalid_batch.append((raw_value.decode('utf-8', errors='replace'), error or "invalid event"))
                            logging.warning("Invalid event routed to DLQ: %s", error)
            except Exception as e:
                logging.error(f"Error reading from consumer: {e}")

            current_time = asyncio.get_event_loop().time()
            time_elapsed = current_time - last_flush_time

            should_flush = (
                len(batch) >= BATCH_SIZE
                or len(invalid_batch) >= BATCH_SIZE
                or (time_elapsed >= BATCH_TIMEOUT_SECONDS and (len(batch) > 0 or len(invalid_batch) > 0))
            )

            if should_flush:
                events_flushed = await flush_to_clickhouse(ch_client, batch)
                if events_flushed:
                    batch.clear()

                dlq_flushed = await flush_to_dlq(ch_client, invalid_batch)
                if dlq_flushed:
                    invalid_batch.clear()

                if events_flushed and dlq_flushed:
                    await commit_offsets(consumer)
                    last_flush_time = current_time

    finally:
        events_flushed = True
        dlq_flushed = True
        if batch:
            logging.info(f"Flushing remaining {len(batch)} items before shutdown...")
            events_flushed = await flush_to_clickhouse(ch_client, batch)
            if events_flushed:
                batch.clear()
        if invalid_batch:
            dlq_flushed = await flush_to_dlq(ch_client, invalid_batch)
            if dlq_flushed:
                invalid_batch.clear()
        if events_flushed and dlq_flushed:
            await commit_offsets(consumer)
        await consumer.stop()
        logging.info("Consumer shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())