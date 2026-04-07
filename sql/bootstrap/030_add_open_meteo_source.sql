\connect workshop;

CREATE TABLE IF NOT EXISTS raw.open_meteo_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.open_meteo_payloads OWNER TO etl;

CREATE INDEX IF NOT EXISTS idx_raw_open_meteo_payloads_loaded_at
    ON raw.open_meteo_payloads (loaded_at DESC);

CREATE TABLE IF NOT EXISTS stg.moscow_weather_daily (
    weather_date DATE PRIMARY KEY,
    temperature_max_c NUMERIC(8, 2),
    temperature_min_c NUMERIC(8, 2),
    precipitation_mm NUMERIC(8, 2),
    wind_speed_max_ms NUMERIC(8, 2),
    weather_regime TEXT NOT NULL,
    is_precipitation_day BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE stg.moscow_weather_daily OWNER TO etl;
