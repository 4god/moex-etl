INSERT INTO vault.sat_currency_rate (
    currency_hk,
    date_hk,
    hashdiff,
    nominal,
    rate,
    load_dts,
    record_source
)
SELECT
    md5(r.char_code) AS currency_hk,
    md5(r.rate_date::TEXT) AS date_hk,
    md5(
        COALESCE(r.nominal::TEXT, '') || '|' ||
        COALESCE(r.rate::TEXT, '')
    ) AS hashdiff,
    r.nominal,
    r.rate,
    NOW() AS load_dts,
    'CBR_STG' AS record_source
FROM stg.cbr_fx_rates r;
