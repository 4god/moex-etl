{{
  config(
    materialized='view',
    schema='analytics',
    alias='fct_cbr_fx_rates',
    tags=['marts', 'source:cbr', 'dataset:stg_cbr']
  )
}}

select
    rate_date,
    char_code,
    nominal,
    rate,
    currency_name,
    updated_at
from {{ ref('stg_cbr_fx_rates') }}
