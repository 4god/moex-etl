TRUNCATE TABLE analytics.dim_moscow_weather_regime_scd2;

WITH ordered_weather AS (
    SELECT
        weather_date,
        weather_regime,
        is_precipitation_day,
        md5(
            COALESCE(weather_regime, '') || '|' ||
            COALESCE(is_precipitation_day::TEXT, '')
        ) AS change_hash
    FROM stg.moscow_weather_daily
    ORDER BY weather_date
),
change_marks AS (
    SELECT
        *,
        CASE
            WHEN LAG(change_hash) OVER (ORDER BY weather_date) = change_hash THEN 0
            ELSE 1
        END AS change_flag
    FROM ordered_weather
),
scd_groups AS (
    SELECT
        *,
        SUM(change_flag) OVER (ORDER BY weather_date) AS scd_group_id
    FROM change_marks
),
scd_ranges AS (
    SELECT
        scd_group_id,
        MIN(weather_date) AS source_start_date,
        MAX(weather_date) AS source_end_date,
        MAX(weather_regime) AS weather_regime,
        BOOL_OR(is_precipitation_day) AS is_precipitation_day
    FROM scd_groups
    GROUP BY scd_group_id
),
final_scd AS (
    SELECT
        'MOSCOW'::TEXT AS city_code,
        weather_regime,
        is_precipitation_day,
        source_start_date AS valid_from,
        CASE
            WHEN LEAD(source_start_date) OVER (ORDER BY source_start_date) IS NULL THEN NULL
            ELSE LEAD(source_start_date) OVER (ORDER BY source_start_date) - 1
        END AS valid_to,
        LEAD(source_start_date) OVER (ORDER BY source_start_date) IS NULL AS is_current,
        ROW_NUMBER() OVER (ORDER BY source_start_date) AS version_num,
        source_start_date,
        source_end_date
    FROM scd_ranges
)
INSERT INTO analytics.dim_moscow_weather_regime_scd2 (
    city_code,
    weather_regime,
    is_precipitation_day,
    valid_from,
    valid_to,
    is_current,
    version_num,
    source_start_date,
    source_end_date,
    created_at
)
SELECT
    city_code,
    weather_regime,
    is_precipitation_day,
    valid_from,
    valid_to,
    is_current,
    version_num,
    source_start_date,
    source_end_date,
    NOW()
FROM final_scd
ORDER BY version_num;
