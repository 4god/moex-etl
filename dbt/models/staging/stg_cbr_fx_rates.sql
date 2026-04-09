{{ config(materialized='view', tags=['staging', 'source:cbr']) }}

select
    rate_date,
    char_code,
    nominal,
    rate,
    currency_name,
    updated_at
from {{ source('stg_pipeline', 'cbr_fx_rates') }}
