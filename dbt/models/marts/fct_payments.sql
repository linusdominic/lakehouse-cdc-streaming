-- Grain: one row per payment that currently exists at the source.
-- loan_status_at_payment is a point-in-time lookup into the SCD2 dimension.
select
    p.payment_id,
    p.loan_id,
    l.borrower_id,
    s.loan_status_key,
    s.status as loan_status_at_payment,
    p.payment_date,
    p.amount,
    p.principal_component,
    p.interest_component,
    p.fee_component,
    p.status as payment_status,
    p.status = 'REVERSED' as is_reversed,
    case when p.status = 'REVERSED' then cast(0 as decimal(14, 2)) else p.amount end as net_amount,
    p.channel,
    p.created_at as posted_at
from {{ ref('stg_payments') }} as p
inner join {{ ref('dim_loan') }} as l
    on l.loan_id = p.loan_id
left join {{ ref('dim_loan_status_history') }} as s
    on s.loan_id = p.loan_id
    and p.created_at >= s.valid_from
    and (p.created_at < s.valid_to or s.valid_to is null)
