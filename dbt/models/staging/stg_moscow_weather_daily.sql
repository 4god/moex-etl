{{ config(materialized='view', tags=['staging', 'source:meteo']) }}

select
    weather_date,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    wind_speed_max_ms,
    weather_regime,
    is_precipitation_day,
    updated_at
from {{ source('stg_pipeline', 'moscow_weather_daily') }}
