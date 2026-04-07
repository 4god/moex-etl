TRUNCATE TABLE stg.moscow_weather_daily;

WITH latest_payload AS (
    SELECT payload
    FROM raw.open_meteo_payloads
    ORDER BY loaded_at DESC
    LIMIT 1
),
daily_time AS (
    SELECT
        value::DATE AS weather_date,
        ordinality
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'daily' -> 'time') WITH ORDINALITY
),
daily_tmax AS (
    SELECT
        NULLIF(value::TEXT, '')::NUMERIC(8, 2) AS temperature_max_c,
        ordinality
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'daily' -> 'temperature_2m_max') WITH ORDINALITY
),
daily_tmin AS (
    SELECT
        NULLIF(value::TEXT, '')::NUMERIC(8, 2) AS temperature_min_c,
        ordinality
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'daily' -> 'temperature_2m_min') WITH ORDINALITY
),
daily_precip AS (
    SELECT
        NULLIF(value::TEXT, '')::NUMERIC(8, 2) AS precipitation_mm,
        ordinality
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'daily' -> 'precipitation_sum') WITH ORDINALITY
),
daily_wind AS (
    SELECT
        NULLIF(value::TEXT, '')::NUMERIC(8, 2) AS wind_speed_max_ms,
        ordinality
    FROM latest_payload,
         jsonb_array_elements_text(payload -> 'daily' -> 'wind_speed_10m_max') WITH ORDINALITY
)
INSERT INTO stg.moscow_weather_daily (
    weather_date,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    wind_speed_max_ms,
    weather_regime,
    is_precipitation_day,
    updated_at
)
SELECT
    t.weather_date,
    tmax.temperature_max_c,
    tmin.temperature_min_c,
    p.precipitation_mm,
    w.wind_speed_max_ms,
    CASE
        WHEN tmax.temperature_max_c < 0 THEN 'freezing'
        WHEN tmax.temperature_max_c < 10 THEN 'cold'
        WHEN tmax.temperature_max_c < 20 THEN 'mild'
        WHEN tmax.temperature_max_c < 28 THEN 'warm'
        ELSE 'hot'
    END AS weather_regime,
    COALESCE(p.precipitation_mm, 0) > 0 AS is_precipitation_day,
    NOW() AS updated_at
FROM daily_time t
JOIN daily_tmax tmax
    ON tmax.ordinality = t.ordinality
JOIN daily_tmin tmin
    ON tmin.ordinality = t.ordinality
LEFT JOIN daily_precip p
    ON p.ordinality = t.ordinality
LEFT JOIN daily_wind w
    ON w.ordinality = t.ordinality
ORDER BY t.weather_date;
