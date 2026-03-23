{{
  config(
    materialized='view',
    schema='analytics',
    alias='fct_cbr_fx_rates'
  )
}}

select
    rate_date,
    char_code,
    nominal,
    rate,
    currency_name,
    updated_at
from {{ source('stg_src', 'cbr_fx_rates') }}
