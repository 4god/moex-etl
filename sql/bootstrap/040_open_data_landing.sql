\connect workshop;

-- Лендинг для произвольных открытых HTTP/API-источников (см. dags/ods_* и config/open_data_sources*.yaml)
CREATE TABLE IF NOT EXISTS raw.open_data_snapshots (
    id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.open_data_snapshots OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_open_data_snapshots_source_loaded
    ON raw.open_data_snapshots (source_key, loaded_at DESC);
