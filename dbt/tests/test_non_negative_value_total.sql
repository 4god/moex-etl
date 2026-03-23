select *
from {{ ref('fct_moex_liquidity') }}
where value_total < 0
