-- Main analytical table using the MergeTree engine with LowCardinality dictionary encoding
CREATE TABLE IF NOT EXISTS default.system_events (
    timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
    service_name LowCardinality(String),
    event_type LowCardinality(String),
    environment LowCardinality(String),
    payload String,
    latency_ms Int32
) ENGINE = MergeTree()
PRIMARY KEY (service_name, event_type)
ORDER BY (service_name, event_type, timestamp);

-- Dead Letter Queue table for isolating malformed raw payloads
CREATE TABLE IF NOT EXISTS default.system_events_dlq (
    timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
    raw_payload String,
    error_message String
) ENGINE = MergeTree()
ORDER BY timestamp;
