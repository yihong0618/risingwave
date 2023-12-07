CREATE TABLE IF NOT EXISTS event (
    source_id VARCHAR,
    data_type VARCHAR,
    value BYTEA
);

CREATE TABLE IF NOT EXISTS host_metrics (
    ts TIMESTAMP,
    collector VARCHAR,
    host VARCHAR,
    val NUMERIC
);
