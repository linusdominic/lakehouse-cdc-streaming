-- Roll rates: of the loans that left a status in a given month, what share
-- went to each next status. Built from consecutive SCD2 versions.
-- Transitions into DELETED are excluded: a hard delete at the source is a
-- data correction, not a credit event.
with transitions as (
    select
        loan_id,
        status as from_status,
        lead(status) over (partition by loan_id order by valid_from_lsn) as to_status,
        valid_to as transitioned_at
    from {{ ref('dim_loan_status_history') }}
),

counted as (
    select
        cast(date_trunc('month', transitioned_at) as date) as transition_month,
        from_status,
        to_status,
        count(*) as loans
    from transitions
    where to_status is not null
        and to_status <> 'DELETED'
    group by 1, 2, 3
)

select
    transition_month,
    from_status,
    to_status,
    loans,
    sum(loans) over (partition by transition_month, from_status) as loans_leaving_status,
    cast(loans as double) / sum(loans) over (partition by transition_month, from_status) as roll_rate
from counted
