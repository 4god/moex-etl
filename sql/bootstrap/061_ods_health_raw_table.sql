\connect workshop;

-- ODS: показатели здоровья/среды (World Bank) — Kafka topic raw.ods.health.payloads → DAG ods_health_worldbank_fetch.
CREATE TABLE IF NOT EXISTS raw.ods_health_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.ods_health_payloads OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_raw_ods_health_loaded_at
    ON raw.ods_health_payloads (loaded_at DESC);
