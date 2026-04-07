INSERT INTO vault.link_security_currency_date (
    link_hk,
    security_hk,
    currency_hk,
    date_hk,
    load_dts,
    record_source
)
SELECT
    md5(hs.security_hk || hc.currency_hk || hd.date_hk) AS link_hk,
    hs.security_hk,
    hc.currency_hk,
    hd.date_hk,
    NOW() AS load_dts,
    'MOEX+CBR' AS record_source
FROM vault.hub_security hs
JOIN vault.hub_currency hc
    ON hc.char_code = 'RUB'
JOIN vault.hub_calendar hd
    ON hd.business_date = CURRENT_DATE;
