select
    loan_id,
    status,
    valid_from,
    valid_from_lsn,
    valid_to,
    valid_to_lsn,
    is_current
from {{ source('silver', 'loan_status_scd2') }}
