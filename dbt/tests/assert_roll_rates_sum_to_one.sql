select transition_month, from_status, sum(roll_rate) as total
from {{ ref('mart_delinquency_roll_rates') }}
group by 1, 2
having abs(sum(roll_rate) - 1.0) > 1e-9
