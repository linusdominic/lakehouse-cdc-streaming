-- Type 2 dimension over loan status. Half-open validity: [valid_from, valid_to).
select
    lower(to_hex(md5(to_utf8(
        cast(loan_id as varchar) || '|' || cast(valid_from_lsn as varchar)
    )))) as loan_status_key,
    loan_id,
    status,
    valid_from,
    valid_to,
    is_current,
    valid_from_lsn
from {{ ref('stg_loan_status_versions') }}
