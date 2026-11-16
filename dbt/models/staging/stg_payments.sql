select
    payment_id,
    loan_id,
    payment_date,
    amount,
    principal_component,
    interest_component,
    fee_component,
    status,
    {{ optional_column(source('silver', 'payments'), 'channel') }},
    created_at,
    updated_at
from {{ source('silver', 'payments') }}
where not _is_deleted
