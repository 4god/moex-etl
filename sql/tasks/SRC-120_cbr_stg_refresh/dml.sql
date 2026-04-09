TRUNCATE TABLE stg.cbr_fx_rates;

-- По одному снимку на календарный день (последний loaded_at при дублях); raw может содержать
-- архивные дни (src_cbr_ingestion с workshop_ods_backfill) и текущий daily_json.js.
WITH best_per_day AS (
    SELECT DISTINCT ON ((payload->>'Date')::timestamptz::date)
        (payload->>'Date')::timestamptz::date AS rate_date,
        payload
    FROM raw.cbr_daily_payloads
    WHERE source = 'CBR_DAILY'
    ORDER BY (payload->>'Date')::timestamptz::date, loaded_at DESC
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
    b.rate_date,
    rates.char_code,
    rates.nominal,
    rates.rate,
    rates.currency_name,
    NOW()
FROM best_per_day b
CROSS JOIN LATERAL (
    SELECT
        kv.key AS char_code,
        (kv.value ->> 'Nominal')::INTEGER AS nominal,
        (kv.value ->> 'Value')::NUMERIC(18, 6) AS rate,
        kv.value ->> 'Name' AS currency_name
    FROM jsonb_each(b.payload -> 'Valute') AS kv(key, value)

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
