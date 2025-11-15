ATTACH TABLE _ UUID '840b9a6a-8551-4c72-9bd0-dd744fca557e'
(
    `event_type` String,
    `total_events` UInt64,
    `success_count` UInt64,
    `error_count` UInt64,
    `avg_latency_ms` Float64
)
ENGINE = MergeTree
ORDER BY event_type
SETTINGS index_granularity = 8192
