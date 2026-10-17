import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
import pytest

from services.consumer import worker


def test_parse_event_valid():
    raw = json.dumps({
        "service_name": "checkout-service",
        "event_type": "payment_processed",
        "environment": ["production"],
        "payload": '{"order_id": 123}',
        "latency_ms": 45,
    }).encode("utf-8")

    event, err = worker.parse_event(raw)
    assert err is None
    assert event is not None
    assert event["service_name"] == "checkout-service"


def test_parse_event_invalid_json():
    event, err = worker.parse_event(b"{bad-json")
    assert event is None
    assert err is not None
    assert "invalid JSON" in err


def test_flush_to_clickhouse_success():
    mock_client = MagicMock()
    batch = [{
        "timestamp": "2026-10-16T12:00:00Z",
        "service_name": "auth-service",
        "event_type": "login",
        "environment": ["production"],
        "payload": "ok",
        "latency_ms": 12,
    }]

    result = asyncio.run(worker.flush_to_clickhouse(mock_client, batch))
    assert result is True
    assert mock_client.insert.call_count == 1
    assert mock_client.insert.call_args.kwargs["table"] == "system_events"


def test_flush_to_clickhouse_falls_back_to_dlq_and_returns_true():
    mock_client = MagicMock()
    # First insert (system_events) fails, second insert (system_events_dlq) succeeds
    mock_client.insert.side_effect = [RuntimeError("DB write error"), None]

    batch = [{
        "service_name": "auth-service",
        "event_type": "login",
        "environment": ["production"],
        "payload": "ok",
        "latency_ms": 12,
    }]

    result = asyncio.run(worker.flush_to_clickhouse(mock_client, batch))
    assert result is True
    assert mock_client.insert.call_count == 2
    assert mock_client.insert.call_args_list[0].kwargs["table"] == "system_events"
    assert mock_client.insert.call_args_list[1].kwargs["table"] == "system_events_dlq"


def test_flush_to_clickhouse_returns_false_when_both_inserts_fail():
    mock_client = MagicMock()
    mock_client.insert.side_effect = RuntimeError("ClickHouse unreachable")

    batch = [{
        "service_name": "auth-service",
        "event_type": "login",
        "environment": ["production"],
        "payload": "ok",
        "latency_ms": 12,
    }]

    result = asyncio.run(worker.flush_to_clickhouse(mock_client, batch))
    assert result is False
    assert mock_client.insert.call_count == 2


def test_manual_commit_only_after_successful_flush():
    """Verifies enable_auto_commit=False and offsets are committed only when flush succeeds."""
    mock_ch_client = MagicMock()
    mock_consumer = AsyncMock()

    valid_msg = MagicMock()
    valid_msg.value = json.dumps({
        "service_name": "payment-service",
        "event_type": "charge",
        "environment": ["production"],
        "payload": "{}",
        "latency_ms": 20,
    }).encode("utf-8")

    # Return 1 message on first poll, then trigger shutdown
    async def getmany_side_effect(*args, **kwargs):
        return {"tp0": [valid_msg]}

    mock_consumer.getmany.side_effect = getmany_side_effect

    with patch("services.consumer.worker.clickhouse_connect.get_client", return_value=mock_ch_client), \
         patch("services.consumer.worker.AIOKafkaConsumer", return_value=mock_consumer) as consumer_cls, \
         patch.object(worker, "BATCH_SIZE", 1):

        # Stop loop after first commit
        async def commit_side_effect():
            raise KeyboardInterrupt()

        mock_consumer.commit.side_effect = commit_side_effect

        try:
            asyncio.run(worker.main())
        except KeyboardInterrupt:
            pass

        # Verify consumer was initialized with enable_auto_commit=False
        consumer_cls.assert_called_once()
        assert consumer_cls.call_args.kwargs["enable_auto_commit"] is False

        # Verify ClickHouse insert happened before commit()
        assert mock_ch_client.insert.called
        mock_consumer.commit.assert_called()


def test_no_offset_commit_when_flush_fails():
    """Verifies consumer.commit() is NOT called if ClickHouse and DLQ writes both fail."""
    mock_ch_client = MagicMock()
    mock_ch_client.insert.side_effect = RuntimeError("ClickHouse completely down")
    mock_consumer = AsyncMock()

    valid_msg = MagicMock()
    valid_msg.value = json.dumps({
        "service_name": "payment-service",
        "event_type": "charge",
        "environment": ["production"],
        "payload": "{}",
        "latency_ms": 20,
    }).encode("utf-8")

    poll_count = 0

    async def getmany_side_effect(*args, **kwargs):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return {"tp0": [valid_msg]}
        raise KeyboardInterrupt()

    mock_consumer.getmany.side_effect = getmany_side_effect

    with patch("services.consumer.worker.clickhouse_connect.get_client", return_value=mock_ch_client), \
         patch("services.consumer.worker.AIOKafkaConsumer", return_value=mock_consumer), \
         patch.object(worker, "BATCH_SIZE", 1):

        try:
            asyncio.run(worker.main())
        except KeyboardInterrupt:
            pass

        # Since both ClickHouse inserts failed, commit() must NEVER be called
        mock_consumer.commit.assert_not_called()
