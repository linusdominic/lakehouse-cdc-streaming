select
    borrower_id,
    full_name,
    state_code,
    credit_score,
    case
        when credit_score >= 740 then 'PRIME_PLUS'
        when credit_score >= 670 then 'PRIME'
        when credit_score >= 580 then 'NEAR_PRIME'
        else 'SUBPRIME'
    end as credit_band,
    annual_income,
    is_active,
    created_at as onboarded_at
from {{ ref('stg_borrowers') }}
