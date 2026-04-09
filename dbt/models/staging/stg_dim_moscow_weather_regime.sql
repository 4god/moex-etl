{{ config(materialized='view', tags=['staging', 'source:meteo', 'layer:analytics']) }}

select
    city_code,
    version_num,
    weather_regime,
    valid_from,
    valid_to,
    is_current,
    is_precipitation_day
from {{ source('analytics_pipeline', 'dim_moscow_weather_regime_scd2') }}
