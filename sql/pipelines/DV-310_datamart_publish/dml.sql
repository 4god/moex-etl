TRUNCATE TABLE datamart.dm_moex_share_overview;

INSERT INTO datamart.dm_moex_share_overview (
    secid,
    shortname,
    boardid,
    lot_size,
    last_price,
    prev_price,
    pct_change,
    num_trades,
    value_total,
    updated_at
)
SELECT
    sp.secid,
    sp.shortname,
    sp.boardid,
    sp.lot_size,
    sm.last_price,
    sm.prev_price,
    sm.pct_change,
    sm.num_trades,
    sm.value_total,
    NOW()
FROM (
    SELECT DISTINCT ON (h.secid)
        h.secid,
        s.shortname,
        s.boardid,
        s.lot_size
    FROM vault.hub_security h
    JOIN vault.sat_security_profile s
        ON s.security_hk = h.security_hk
    ORDER BY h.secid, s.load_dts DESC
) sp
LEFT JOIN (
    SELECT DISTINCT ON (security_hk)
        security_hk,
        last_price,
        prev_price,
        pct_change,
        num_trades,
        value_total
    FROM vault.sat_security_market
    ORDER BY security_hk, load_dts DESC
) sm
    ON sm.security_hk = md5(sp.secid);

TRUNCATE TABLE datamart.dm_security_snapshot;

WITH latest_fx_date AS (
    SELECT MAX(hd.business_date) AS fx_date
    FROM vault.sat_currency_rate scr
    JOIN vault.hub_calendar hd
        ON hd.date_hk = scr.date_hk
),
usd AS (
    SELECT
        scr.rate,
        scr.nominal
    FROM vault.sat_currency_rate scr
    JOIN vault.hub_currency hc
        ON hc.currency_hk = scr.currency_hk
    JOIN vault.hub_calendar hd
        ON hd.date_hk = scr.date_hk
    JOIN latest_fx_date lfd
        ON hd.business_date = lfd.fx_date
    WHERE hc.char_code = 'USD'
    ORDER BY scr.load_dts DESC
    LIMIT 1
),
eur AS (
    SELECT
        scr.rate,
        scr.nominal
    FROM vault.sat_currency_rate scr
    JOIN vault.hub_currency hc
        ON hc.currency_hk = scr.currency_hk
    JOIN vault.hub_calendar hd
        ON hd.date_hk = scr.date_hk
    JOIN latest_fx_date lfd
        ON hd.business_date = lfd.fx_date
    WHERE hc.char_code = 'EUR'
    ORDER BY scr.load_dts DESC
    LIMIT 1
)
INSERT INTO datamart.dm_security_snapshot (
    secid,
    shortname,
    boardid,
    trade_date,
    last_price_rub,
    usd_rate,
    eur_rate,
    last_price_usd,
    last_price_eur,
    pct_change,
    num_trades,
    value_total,
    updated_at
)
SELECT
    d.secid,
    d.shortname,
    d.boardid,
    CURRENT_DATE AS trade_date,
    d.last_price AS last_price_rub,
    usd.rate AS usd_rate,
    eur.rate AS eur_rate,
    CASE
        WHEN usd.rate IS NULL OR usd.rate = 0 OR d.last_price IS NULL THEN NULL
        ELSE ROUND(d.last_price / (usd.rate / usd.nominal), 6)
    END AS last_price_usd,
    CASE
        WHEN eur.rate IS NULL OR eur.rate = 0 OR d.last_price IS NULL THEN NULL
        ELSE ROUND(d.last_price / (eur.rate / eur.nominal), 6)
    END AS last_price_eur,
    d.pct_change,
    d.num_trades,
    d.value_total,
    NOW()
FROM datamart.dm_moex_share_overview d
CROSS JOIN usd
CROSS JOIN eur;
