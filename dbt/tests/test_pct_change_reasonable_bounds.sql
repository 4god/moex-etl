select *
from {{ ref('fct_moex_movers') }}
where pct_change < -50
   or pct_change > 50
