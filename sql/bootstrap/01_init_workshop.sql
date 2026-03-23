\connect workshop;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS datamart;

CREATE TABLE IF NOT EXISTS raw.moex_iss_payloads (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_moex_payloads_loaded_at
    ON raw.moex_iss_payloads (loaded_at DESC);

CREATE TABLE IF NOT EXISTS stg.moex_securities (
    secid TEXT PRIMARY KEY,
    shortname TEXT,
    boardid TEXT,
    lot_size INTEGER,
    secname TEXT,
    regnumber TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.moex_marketdata (
    secid TEXT PRIMARY KEY,
    boardid TEXT,
    last_price NUMERIC(18,6),
    prev_price NUMERIC(18,6),
    market_price_24h NUMERIC(18,6),
    num_trades INTEGER,
    value_total NUMERIC(18,2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS datamart.dm_moex_share_overview (
    secid TEXT PRIMARY KEY,
    shortname TEXT,
    boardid TEXT,
    lot_size INTEGER,
    last_price NUMERIC(18,6),
    prev_price NUMERIC(18,6),
    pct_change NUMERIC(18,4),
    num_trades INTEGER,
    value_total NUMERIC(18,2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
