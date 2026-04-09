\connect workshop;

-- Те же поля, что у прочих raw.* после ingestion из Kafka (см. 010, 020).
CREATE TABLE IF NOT EXISTS raw.ods_economic_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.ods_economic_payloads OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_raw_ods_economic_loaded_at
    ON raw.ods_economic_payloads (loaded_at DESC);

CREATE TABLE IF NOT EXISTS raw.ods_territory_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.ods_territory_payloads OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_raw_ods_territory_loaded_at
    ON raw.ods_territory_payloads (loaded_at DESC);
