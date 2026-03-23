TRUNCATE TABLE stg.moex_securities;

WITH latest_payload AS (
    SELECT payload
    FROM raw.moex_iss_payloads
    ORDER BY loaded_at DESC
    LIMIT 1
),
sec_cols AS (
    SELECT value::text AS col_name, ordinality - 1 AS idx
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'securities' -> 'columns') WITH ORDINALITY
),
sec_idx AS (
    SELECT
        MAX(CASE WHEN col_name = 'SECID' THEN idx END) AS secid_idx,
        MAX(CASE WHEN col_name = 'SHORTNAME' THEN idx END) AS shortname_idx,
        MAX(CASE WHEN col_name = 'BOARDID' THEN idx END) AS boardid_idx,
        MAX(CASE WHEN col_name = 'LOTSIZE' THEN idx END) AS lotsize_idx,
        MAX(CASE WHEN col_name = 'SECNAME' THEN idx END) AS secname_idx,
        MAX(CASE WHEN col_name = 'REGNUMBER' THEN idx END) AS regnumber_idx
    FROM sec_cols
)
INSERT INTO stg.moex_securities (
    secid,
    shortname,
    boardid,
    lot_size,
    secname,
    regnumber,
    updated_at
)
SELECT
    row_data ->> sec_idx.secid_idx AS secid,
    row_data ->> sec_idx.shortname_idx AS shortname,
    row_data ->> sec_idx.boardid_idx AS boardid,
    NULLIF(row_data ->> sec_idx.lotsize_idx, '')::INTEGER AS lot_size,
    row_data ->> sec_idx.secname_idx AS secname,
    row_data ->> sec_idx.regnumber_idx AS regnumber,
    NOW() AS updated_at
FROM latest_payload, sec_idx,
     jsonb_array_elements(payload -> 'securities' -> 'data') AS row_data
WHERE row_data ->> sec_idx.secid_idx IS NOT NULL
ON CONFLICT (secid) DO UPDATE SET
    shortname = EXCLUDED.shortname,
    boardid = EXCLUDED.boardid,
    lot_size = EXCLUDED.lot_size,
    secname = EXCLUDED.secname,
    regnumber = EXCLUDED.regnumber,
    updated_at = NOW();

TRUNCATE TABLE stg.moex_marketdata;

WITH latest_payload AS (
    SELECT payload
    FROM raw.moex_iss_payloads
    ORDER BY loaded_at DESC
    LIMIT 1
),
md_cols AS (
    SELECT value::text AS col_name, ordinality - 1 AS idx
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'marketdata' -> 'columns') WITH ORDINALITY
),
md_idx AS (
    SELECT
        MAX(CASE WHEN col_name = 'SECID' THEN idx END) AS secid_idx,
        MAX(CASE WHEN col_name = 'BOARDID' THEN idx END) AS boardid_idx,
        MAX(CASE WHEN col_name = 'LAST' THEN idx END) AS last_idx,
        MAX(CASE WHEN col_name = 'PREVPRICE' THEN idx END) AS prevprice_idx,
        MAX(CASE WHEN col_name = 'MARKETPRICETODAY' THEN idx END) AS marketprice_idx,
        MAX(CASE WHEN col_name = 'NUMTRADES' THEN idx END) AS numtrades_idx,
        MAX(CASE WHEN col_name = 'VALTODAY' THEN idx END) AS valtoday_idx
    FROM md_cols
)
INSERT INTO stg.moex_marketdata (
    secid,
    boardid,
    last_price,
    prev_price,
    market_price_24h,
    num_trades,
    value_total,
    updated_at
)
SELECT
    row_data ->> md_idx.secid_idx AS secid,
    row_data ->> md_idx.boardid_idx AS boardid,
    NULLIF(row_data ->> md_idx.last_idx, '')::NUMERIC(18,6) AS last_price,
    NULLIF(row_data ->> md_idx.prevprice_idx, '')::NUMERIC(18,6) AS prev_price,
    NULLIF(row_data ->> md_idx.marketprice_idx, '')::NUMERIC(18,6) AS market_price_24h,
    NULLIF(row_data ->> md_idx.numtrades_idx, '')::INTEGER AS num_trades,
    NULLIF(row_data ->> md_idx.valtoday_idx, '')::NUMERIC(18,2) AS value_total,
    NOW() AS updated_at
FROM latest_payload, md_idx,
     jsonb_array_elements(payload -> 'marketdata' -> 'data') AS row_data
WHERE row_data ->> md_idx.secid_idx IS NOT NULL
ON CONFLICT (secid) DO UPDATE SET
    boardid = EXCLUDED.boardid,
    last_price = EXCLUDED.last_price,
    prev_price = EXCLUDED.prev_price,
    market_price_24h = EXCLUDED.market_price_24h,
    num_trades = EXCLUDED.num_trades,
    value_total = EXCLUDED.value_total,
    updated_at = NOW();
