{{
  config(
    materialized='view',
    schema='analytics',
    alias='fct_moex_weather_snapshot',
    tags=['marts', 'cross_source', 'dataset:datamart', 'dataset:weather_dim']
  )
}}

select
    dm.trade_date,
    dm.secid,
    dm.shortname,
    dm.boardid,
    dm.last_price_rub,
    dm.pct_change,
    dm.value_total,
    w.weather_regime,
    w.is_precipitation_day
from {{ ref('stg_dm_security_snapshot') }} dm
left join {{ ref('stg_dim_moscow_weather_regime') }} w
    on w.city_code = 'MOSCOW'
   and dm.trade_date between w.valid_from and coalesce(w.valid_to, DATE '2999-12-31')
