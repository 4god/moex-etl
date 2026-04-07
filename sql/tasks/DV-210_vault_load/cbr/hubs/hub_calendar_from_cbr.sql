INSERT INTO vault.hub_calendar (date_hk, business_date, load_dts, record_source)
SELECT
    md5(business_date::TEXT) AS date_hk,
    business_date,
    NOW() AS load_dts,
    'CBR_STG' AS record_source
FROM (
    SELECT DISTINCT rate_date AS business_date
    FROM stg.cbr_fx_rates
    UNION
    SELECT CURRENT_DATE
) t;
