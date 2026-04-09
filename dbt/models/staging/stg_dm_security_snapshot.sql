{{ config(materialized='view', tags=['staging', 'source:moex', 'source:cbr', 'layer:datamart']) }}

select
    trade_date,
    secid,
    shortname,
    boardid,
    last_price_rub,
    usd_rate,
    eur_rate,
    last_price_usd,
    last_price_eur,
    pct_change,
    num_trades,
    value_total,
    updated_at
from {{ source('datamart_pipeline', 'dm_security_snapshot') }}
