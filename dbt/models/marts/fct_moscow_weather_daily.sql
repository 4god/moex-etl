{{
  config(
    materialized='view',
    schema='analytics',
    alias='fct_moscow_weather_daily',
    tags=['marts', 'source:meteo', 'dataset:stg_meteo']
  )
}}

select
    weather_date,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    wind_speed_max_ms,
    weather_regime,
    is_precipitation_day,
    updated_at
from {{ ref('stg_moscow_weather_daily') }}
