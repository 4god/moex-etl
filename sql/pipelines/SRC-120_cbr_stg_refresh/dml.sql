TRUNCATE TABLE stg.cbr_fx_rates;

WITH latest_payload AS (
    SELECT payload
    FROM raw.cbr_daily_payloads
    ORDER BY loaded_at DESC
    LIMIT 1
),
cbr_date AS (
    SELECT (payload ->> 'Date')::TIMESTAMPTZ::DATE AS rate_date
    FROM latest_payload
)
INSERT INTO stg.cbr_fx_rates (
    rate_date,
    char_code,
    nominal,
    rate,
    currency_name,
    updated_at
)
SELECT
    d.rate_date,
    rates.char_code,
    rates.nominal,
    rates.rate,
    rates.currency_name,
    NOW()
FROM cbr_date d
CROSS JOIN LATERAL (
    SELECT
        kv.key AS char_code,
        (kv.value ->> 'Nominal')::INTEGER AS nominal,
        (kv.value ->> 'Value')::NUMERIC(18, 6) AS rate,
        kv.value ->> 'Name' AS currency_name
    FROM latest_payload lp,
         LATERAL jsonb_each(lp.payload -> 'Valute') kv

    UNION ALL

    SELECT
        'RUB'::TEXT AS char_code,
        1::INTEGER AS nominal,
        1::NUMERIC(18, 6) AS rate,
        'Russian Ruble'::TEXT AS currency_name
) AS rates
ON CONFLICT (rate_date, char_code) DO UPDATE SET
    nominal = EXCLUDED.nominal,
    rate = EXCLUDED.rate,
    currency_name = EXCLUDED.currency_name,
    updated_at = NOW();
