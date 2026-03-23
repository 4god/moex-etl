INSERT INTO vault.sat_security_profile (
    security_hk,
    hashdiff,
    shortname,
    boardid,
    lot_size,
    secname,
    regnumber,
    load_dts,
    record_source
)
SELECT
    md5(s.secid) AS security_hk,
    md5(
        COALESCE(s.shortname, '') || '|' ||
        COALESCE(s.boardid, '') || '|' ||
        COALESCE(s.lot_size::TEXT, '') || '|' ||
        COALESCE(s.secname, '') || '|' ||
        COALESCE(s.regnumber, '')
    ) AS hashdiff,
    s.shortname,
    s.boardid,
    s.lot_size,
    s.secname,
    s.regnumber,
    NOW() AS load_dts,
    'MOEX_STG' AS record_source
FROM stg.moex_securities s;
