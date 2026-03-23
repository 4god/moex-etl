CREATE TABLE IF NOT EXISTS analytics.dim_moscow_weather_regime_scd2 (
    weather_scd_id BIGSERIAL PRIMARY KEY,
    city_code TEXT NOT NULL,
    weather_regime TEXT NOT NULL,
    is_precipitation_day BOOLEAN NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL,
    version_num INTEGER NOT NULL,
    source_start_date DATE NOT NULL,
    source_end_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_scd2_city_valid_from
    ON analytics.dim_moscow_weather_regime_scd2 (city_code, valid_from);
