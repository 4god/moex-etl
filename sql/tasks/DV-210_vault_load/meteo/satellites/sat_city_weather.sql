DO $$
BEGIN
    IF to_regclass('stg.moscow_weather_daily') IS NOT NULL THEN
        INSERT INTO vault.sat_city_weather (
            city_hk,
            date_hk,
            hashdiff,
            temperature_max_c,
            temperature_min_c,
            precipitation_mm,
            wind_speed_max_ms,
            weather_regime,
            is_precipitation_day,
            load_dts,
            record_source
        )
        SELECT
            md5('MOSCOW') AS city_hk,
            md5(w.weather_date::TEXT) AS date_hk,
            md5(
                COALESCE(w.temperature_max_c::TEXT, '') || '|' ||
                COALESCE(w.temperature_min_c::TEXT, '') || '|' ||
                COALESCE(w.precipitation_mm::TEXT, '') || '|' ||
                COALESCE(w.wind_speed_max_ms::TEXT, '') || '|' ||
                COALESCE(w.weather_regime, '') || '|' ||
                COALESCE(w.is_precipitation_day::TEXT, '')
            ) AS hashdiff,
            w.temperature_max_c,
            w.temperature_min_c,
            w.precipitation_mm,
            w.wind_speed_max_ms,
            w.weather_regime,
            w.is_precipitation_day,
            NOW() AS load_dts,
            'OPEN_METEO_STG' AS record_source
        FROM stg.moscow_weather_daily w;
    END IF;
END $$;
