INSERT INTO vault.hub_currency (currency_hk, char_code, load_dts, record_source)
SELECT
    md5(char_code) AS currency_hk,
    char_code,
    NOW() AS load_dts,
    'CBR_STG' AS record_source
FROM (
    SELECT DISTINCT char_code
    FROM stg.cbr_fx_rates
) t;
