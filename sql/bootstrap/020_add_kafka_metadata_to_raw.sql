\connect workshop;

ALTER TABLE IF EXISTS raw.moex_iss_payloads
    ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

ALTER TABLE IF EXISTS raw.cbr_daily_payloads
    ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;
