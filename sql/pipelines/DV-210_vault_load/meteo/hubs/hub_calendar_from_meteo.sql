DO $$
BEGIN
    IF to_regclass('stg.moscow_weather_daily') IS NOT NULL THEN
        INSERT INTO vault.hub_calendar (date_hk, business_date, load_dts, record_source)
        SELECT
            md5(weather_date::TEXT) AS date_hk,
            weather_date AS business_date,
            NOW() AS load_dts,
            'OPEN_METEO_STG' AS record_source
        FROM stg.moscow_weather_daily
        ON CONFLICT (business_date) DO NOTHING;
    END IF;
END $$;
