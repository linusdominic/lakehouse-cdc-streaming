select
    loan_id,
    borrower_id,
    product_type,
    property_state,
    original_principal,
    principal_balance,
    interest_rate,
    term_months,
    origination_date,
    next_due_date,
    status,
    created_at,
    updated_at
from {{ source('silver', 'loans') }}
where not _is_deleted
