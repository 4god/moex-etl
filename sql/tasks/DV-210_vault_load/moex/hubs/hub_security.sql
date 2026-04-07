INSERT INTO vault.hub_security (security_hk, secid, load_dts, record_source)
SELECT
    md5(secid) AS security_hk,
    secid,
    NOW() AS load_dts,
    'MOEX_STG' AS record_source
FROM stg.moex_securities;
