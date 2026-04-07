\connect workshop;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS vault;
CREATE SCHEMA IF NOT EXISTS datamart;
CREATE SCHEMA IF NOT EXISTS analytics;

ALTER SCHEMA raw OWNER TO etl;
ALTER SCHEMA stg OWNER TO etl;
ALTER SCHEMA vault OWNER TO etl;
ALTER SCHEMA datamart OWNER TO etl;
ALTER SCHEMA analytics OWNER TO etl;

GRANT USAGE ON SCHEMA raw, stg, vault, datamart, analytics TO etl;
GRANT CREATE ON SCHEMA raw, stg, vault, datamart, analytics TO etl;

CREATE TABLE IF NOT EXISTS raw.moex_iss_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.moex_iss_payloads OWNER TO etl;

CREATE TABLE IF NOT EXISTS raw.cbr_daily_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE raw.cbr_daily_payloads OWNER TO etl;

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

CREATE INDEX IF NOT EXISTS idx_raw_moex_payloads_loaded_at
    ON raw.moex_iss_payloads (loaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_cbr_payloads_loaded_at
    ON raw.cbr_daily_payloads (loaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_open_meteo_payloads_loaded_at
    ON raw.open_meteo_payloads (loaded_at DESC);

CREATE TABLE IF NOT EXISTS stg.moex_securities (
    secid TEXT PRIMARY KEY,
    shortname TEXT,
    boardid TEXT,
    lot_size INTEGER,
    secname TEXT,
    regnumber TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE stg.moex_securities OWNER TO etl;

CREATE TABLE IF NOT EXISTS stg.moex_marketdata (
    secid TEXT PRIMARY KEY,
    boardid TEXT,
    last_price NUMERIC(18, 6),
    prev_price NUMERIC(18, 6),
    market_price_24h NUMERIC(18, 6),
    num_trades INTEGER,
    value_total NUMERIC(18, 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE stg.moex_marketdata OWNER TO etl;

CREATE TABLE IF NOT EXISTS stg.cbr_fx_rates (
    rate_date DATE NOT NULL,
    char_code TEXT NOT NULL,
    nominal INTEGER NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    currency_name TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rate_date, char_code)
);
ALTER TABLE stg.cbr_fx_rates OWNER TO etl;

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
