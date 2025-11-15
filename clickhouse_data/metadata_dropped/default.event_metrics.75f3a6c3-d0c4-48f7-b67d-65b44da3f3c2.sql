ATTACH TABLE _ UUID '75f3a6c3-d0c4-48f7-b67d-65b44da3f3c2'
(
    `id` UInt64,
    `user_id` String,
    `session_id` String,
    `event_type` String,
    `event_timestamp` DateTime,
    `request_latency_ms` UInt32,
    `status` String,
    `error_code` Nullable(String),
    `product_id` Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY (event_type, event_timestamp)
SETTINGS index_granularity = 8192
