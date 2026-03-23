CREATE TABLE IF NOT EXISTS stg.moscow_weather_daily (
    weather_date DATE PRIMARY KEY,
    temperature_max_c NUMERIC(8, 2),
    temperature_min_c NUMERIC(8, 2),
    precipitation_mm NUMERIC(8, 2),
    wind_speed_max_ms NUMERIC(8, 2),
    weather_regime TEXT NOT NULL,
    is_precipitation_day BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
