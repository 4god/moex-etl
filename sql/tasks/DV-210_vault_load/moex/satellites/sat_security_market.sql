INSERT INTO vault.sat_security_market (
    security_hk,
    date_hk,
    hashdiff,
    last_price,
    prev_price,
    market_price_24h,
    pct_change,
    num_trades,
    value_total,
    load_dts,
    record_source
)
SELECT
    md5(m.secid) AS security_hk,
    md5(CURRENT_DATE::TEXT) AS date_hk,
    md5(
        COALESCE(m.last_price::TEXT, '') || '|' ||
        COALESCE(m.prev_price::TEXT, '') || '|' ||
        COALESCE(m.market_price_24h::TEXT, '') || '|' ||
        COALESCE(m.num_trades::TEXT, '') || '|' ||
        COALESCE(m.value_total::TEXT, '')
    ) AS hashdiff,
    m.last_price,
    m.prev_price,
    m.market_price_24h,
    CASE
        WHEN m.prev_price IS NULL OR m.prev_price = 0 OR m.last_price IS NULL THEN NULL
        ELSE ROUND(((m.last_price - m.prev_price) / m.prev_price) * 100, 4)
    END AS pct_change,
    m.num_trades,
    m.value_total,
    NOW() AS load_dts,
    'MOEX_STG' AS record_source
FROM stg.moex_marketdata m;
