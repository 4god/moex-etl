\connect workshop;

-- ODS: маркетинг / потребитель (Eurostat + World Bank) — DAG ods_marketing_insight_fetch.
CREATE TABLE IF NOT EXISTS raw.ods_marketing_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.ods_marketing_payloads OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_raw_ods_marketing_loaded_at
    ON raw.ods_marketing_payloads (loaded_at DESC);
