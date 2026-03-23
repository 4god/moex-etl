CREATE TABLE IF NOT EXISTS datamart.dm_moex_share_overview (
    secid TEXT PRIMARY KEY,
    shortname TEXT,
    boardid TEXT,
    lot_size INTEGER,
    last_price NUMERIC(18, 6),
    prev_price NUMERIC(18, 6),
    pct_change NUMERIC(18, 4),
    num_trades INTEGER,
    value_total NUMERIC(18, 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS datamart.dm_security_snapshot (
    secid TEXT PRIMARY KEY,
    shortname TEXT,
    boardid TEXT,
    trade_date DATE NOT NULL,
    last_price_rub NUMERIC(18, 6),
    usd_rate NUMERIC(18, 6),
    eur_rate NUMERIC(18, 6),
    last_price_usd NUMERIC(18, 6),
    last_price_eur NUMERIC(18, 6),
    pct_change NUMERIC(18, 4),
    num_trades INTEGER,
    value_total NUMERIC(18, 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
