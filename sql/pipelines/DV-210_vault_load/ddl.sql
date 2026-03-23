CREATE TABLE IF NOT EXISTS vault.hub_security (
    security_hk TEXT PRIMARY KEY,
    secid TEXT NOT NULL UNIQUE,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vault.hub_currency (
    currency_hk TEXT PRIMARY KEY,
    char_code TEXT NOT NULL UNIQUE,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vault.hub_calendar (
    date_hk TEXT PRIMARY KEY,
    business_date DATE NOT NULL UNIQUE,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vault.link_security_currency_date (
    link_hk TEXT PRIMARY KEY,
    security_hk TEXT NOT NULL REFERENCES vault.hub_security(security_hk),
    currency_hk TEXT NOT NULL REFERENCES vault.hub_currency(currency_hk),
    date_hk TEXT NOT NULL REFERENCES vault.hub_calendar(date_hk),
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    UNIQUE (security_hk, currency_hk, date_hk)
);

CREATE TABLE IF NOT EXISTS vault.sat_security_profile (
    security_hk TEXT NOT NULL REFERENCES vault.hub_security(security_hk),
    hashdiff TEXT NOT NULL,
    shortname TEXT,
    boardid TEXT,
    lot_size INTEGER,
    secname TEXT,
    regnumber TEXT,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    PRIMARY KEY (security_hk, load_dts)
);

CREATE TABLE IF NOT EXISTS vault.sat_security_market (
    security_hk TEXT NOT NULL REFERENCES vault.hub_security(security_hk),
    date_hk TEXT NOT NULL REFERENCES vault.hub_calendar(date_hk),
    hashdiff TEXT NOT NULL,
    last_price NUMERIC(18, 6),
    prev_price NUMERIC(18, 6),
    market_price_24h NUMERIC(18, 6),
    pct_change NUMERIC(18, 4),
    num_trades INTEGER,
    value_total NUMERIC(18, 2),
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    PRIMARY KEY (security_hk, date_hk, load_dts)
);

CREATE TABLE IF NOT EXISTS vault.sat_currency_rate (
    currency_hk TEXT NOT NULL REFERENCES vault.hub_currency(currency_hk),
    date_hk TEXT NOT NULL REFERENCES vault.hub_calendar(date_hk),
    hashdiff TEXT NOT NULL,
    nominal INTEGER NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    PRIMARY KEY (currency_hk, date_hk, load_dts)
);

CREATE TABLE IF NOT EXISTS vault.hub_city (
    city_hk TEXT PRIMARY KEY,
    city_code TEXT NOT NULL UNIQUE,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vault.link_city_date (
    link_city_date_hk TEXT PRIMARY KEY,
    city_hk TEXT NOT NULL REFERENCES vault.hub_city(city_hk),
    date_hk TEXT NOT NULL REFERENCES vault.hub_calendar(date_hk),
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    UNIQUE (city_hk, date_hk)
);

CREATE TABLE IF NOT EXISTS vault.sat_city_weather (
    city_hk TEXT NOT NULL REFERENCES vault.hub_city(city_hk),
    date_hk TEXT NOT NULL REFERENCES vault.hub_calendar(date_hk),
    hashdiff TEXT NOT NULL,
    temperature_max_c NUMERIC(8, 2),
    temperature_min_c NUMERIC(8, 2),
    precipitation_mm NUMERIC(8, 2),
    wind_speed_max_ms NUMERIC(8, 2),
    weather_regime TEXT,
    is_precipitation_day BOOLEAN NOT NULL,
    load_dts TIMESTAMPTZ NOT NULL,
    record_source TEXT NOT NULL,
    PRIMARY KEY (city_hk, date_hk, load_dts)
);
