-- For each loan the status intervals must tile time: every version except the
-- last ends exactly where the next begins, and exactly one version is current.
with ordered as (
    select
        loan_id,
        valid_to,
        is_current,
        lead(valid_from) over (partition by loan_id order by valid_from_lsn) as next_valid_from
    from {{ ref('dim_loan_status_history') }}
)

select loan_id, 'gap or overlap between versions' as problem
from ordered
where next_valid_from is not null
    and (valid_to is null or valid_to <> next_valid_from)

union all

select loan_id, 'not exactly one current version' as problem
from ordered
group by loan_id
having count_if(is_current) <> 1
