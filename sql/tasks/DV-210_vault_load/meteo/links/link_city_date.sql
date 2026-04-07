DO $$
BEGIN
    IF to_regclass('stg.moscow_weather_daily') IS NOT NULL THEN
        INSERT INTO vault.link_city_date (
            link_city_date_hk,
            city_hk,
            date_hk,
            load_dts,
            record_source
        )
        SELECT
            md5(md5('MOSCOW') || md5(w.weather_date::TEXT)) AS link_city_date_hk,
            md5('MOSCOW') AS city_hk,
            md5(w.weather_date::TEXT) AS date_hk,
            NOW() AS load_dts,
            'OPEN_METEO_STG' AS record_source
        FROM stg.moscow_weather_daily w;
    END IF;
END $$;
