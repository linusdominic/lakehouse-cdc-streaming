-- A payment's components must add up to its amount.
select payment_id, amount, principal_component, interest_component, fee_component
from {{ ref('fct_payments') }}
where amount <> principal_component + interest_component + fee_component
