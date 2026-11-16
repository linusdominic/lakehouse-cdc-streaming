select
    l.loan_id,
    l.borrower_id,
    l.product_type,
    l.property_state,
    l.original_principal,
    l.principal_balance,
    l.interest_rate,
    l.term_months,
    l.origination_date,
    l.next_due_date,
    l.status as current_status,
    l.status in ('DPD_30', 'DPD_60', 'DPD_90', 'DEFAULT') as is_delinquent,
    l.status in ('PAID_OFF', 'CHARGED_OFF') as is_closed,
    cast(1 - l.principal_balance / l.original_principal as decimal(9, 6)) as paid_down_ratio,
    l.updated_at as source_updated_at
from {{ ref('stg_loans') }} as l
