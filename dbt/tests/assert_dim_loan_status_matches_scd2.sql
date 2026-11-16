-- dim_loan.current_status comes from the current-state MERGE; the SCD2
-- dimension is built by a separate code path. They must agree.
select l.loan_id, l.current_status, h.status as scd2_status
from {{ ref('dim_loan') }} as l
left join {{ ref('dim_loan_status_history') }} as h
    on h.loan_id = l.loan_id and h.is_current
where h.status is null or h.status <> l.current_status
