select
    trade_date,
    secid,
    count(*) as row_cnt
from {{ ref('fct_moex_liquidity') }}
group by trade_date, secid
having count(*) > 1
