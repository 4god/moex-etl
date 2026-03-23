{{
  config(
    materialized='incremental',
    schema='analytics',
    alias='fct_moex_liquidity',
    unique_key=['trade_date', 'secid'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    pre_hook=[
      "{% if is_incremental() %}DELETE FROM {{ this }} WHERE trade_date = CURRENT_DATE{% else %}SELECT 1{% endif %}"
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_fct_moex_liquidity_trade_date ON {{ this }} (trade_date)",
      "CREATE INDEX IF NOT EXISTS idx_fct_moex_liquidity_secid ON {{ this }} (secid)",
      "ANALYZE {{ this }}"
    ]
  )
}}

select
    trade_date,
    secid,
    shortname,
    boardid,
    value_total,
    num_trades,
    last_price_rub,
    last_price_usd,
    last_price_eur,
    updated_at
from {{ source('datamart_src', 'dm_security_snapshot') }}
where value_total is not null

{% if is_incremental() %}
  and trade_date = current_date
{% endif %}
