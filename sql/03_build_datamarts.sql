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
    s.secid,
    s.shortname,
    COALESCE(m.boardid, s.boardid) AS boardid,
    s.lot_size,
    m.last_price,
    m.prev_price,
    CASE
        WHEN m.prev_price IS NULL OR m.prev_price = 0 THEN NULL
        WHEN m.last_price IS NULL THEN NULL
        ELSE ROUND(((m.last_price - m.prev_price) / m.prev_price) * 100, 4)
    END AS pct_change,
    m.num_trades,
    m.value_total,
    NOW() AS updated_at
FROM stg.moex_securities s
LEFT JOIN stg.moex_marketdata m
    ON s.secid = m.secid;
