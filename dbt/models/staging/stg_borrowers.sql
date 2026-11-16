select
    borrower_id,
    full_name,
    email,
    state_code,
    credit_score,
    annual_income,
    is_active,
    created_at,
    updated_at
from {{ source('silver', 'borrowers') }}
where not _is_deleted
