{{
  config(
    materialized='incremental',
    schema='analytics',
    alias='fct_moex_movers',
    unique_key=['trade_date', 'secid'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    pre_hook=[
      "{% if is_incremental() %}DELETE FROM {{ this }} WHERE trade_date = CURRENT_DATE{% else %}SELECT 1{% endif %}"
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_fct_moex_movers_trade_date ON {{ this }} (trade_date)",
      "CREATE INDEX IF NOT EXISTS idx_fct_moex_movers_secid ON {{ this }} (secid)",
      "ANALYZE {{ this }}"
    ]
  )
}}

select
    trade_date,
    secid,
    shortname,
    boardid,
    last_price_rub,
    last_price_usd,
    last_price_eur,
    pct_change,
    updated_at
from {{ source('datamart_src', 'dm_security_snapshot') }}
where pct_change is not null

{% if is_incremental() %}
  and trade_date = current_date
{% endif %}
