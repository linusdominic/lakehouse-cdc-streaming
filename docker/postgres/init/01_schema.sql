-- Loan-servicing OLTP schema. Primary keys are assigned by the application
-- (the workload generator), not by sequences, so a run is reproducible.

CREATE TABLE borrowers (
    borrower_id   bigint PRIMARY KEY,
    full_name     text           NOT NULL,
    email         text           NOT NULL,
    state_code    text           NOT NULL,
    credit_score  integer        NOT NULL CHECK (credit_score BETWEEN 300 AND 850),
    annual_income numeric(14, 2) NOT NULL,
    is_active     boolean        NOT NULL DEFAULT true,
    created_at    timestamptz    NOT NULL DEFAULT now(),
    updated_at    timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE loans (
    loan_id            bigint PRIMARY KEY,
    borrower_id        bigint         NOT NULL REFERENCES borrowers (borrower_id),
    product_type       text           NOT NULL,
    property_state     text           NOT NULL,
    original_principal numeric(14, 2) NOT NULL,
    principal_balance  numeric(14, 2) NOT NULL CHECK (principal_balance >= 0),
    interest_rate      numeric(7, 5)  NOT NULL,
    term_months        integer        NOT NULL,
    origination_date   date           NOT NULL,
    next_due_date      date,
    status             text           NOT NULL,
    created_at         timestamptz    NOT NULL DEFAULT now(),
    updated_at         timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id          bigint PRIMARY KEY,
    loan_id             bigint         NOT NULL REFERENCES loans (loan_id),
    payment_date        date           NOT NULL,
    amount              numeric(14, 2) NOT NULL,
    principal_component numeric(14, 2) NOT NULL,
    interest_component  numeric(14, 2) NOT NULL,
    fee_component       numeric(14, 2) NOT NULL,
    status              text           NOT NULL,
    created_at          timestamptz    NOT NULL DEFAULT now(),
    updated_at          timestamptz    NOT NULL DEFAULT now()
);
CREATE INDEX payments_loan_id_idx ON payments (loan_id);

-- Written by the application in the same transaction as every status change.
-- The pipeline does not use it to build the SCD2 table; the verifier uses it
-- as an independent record of what the SCD2 table should contain.
CREATE TABLE loan_status_history (
    history_id  bigint PRIMARY KEY,
    loan_id     bigint      NOT NULL REFERENCES loans (loan_id),
    from_status text,
    to_status   text        NOT NULL,
    reason      text,
    changed_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX loan_status_history_loan_id_idx ON loan_status_history (loan_id);

-- The drain check inserts a row here and waits for it to reach the lake.
CREATE TABLE pipeline_marker (
    marker_id  uuid PRIMARY KEY,
    note       text,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- The SCD2 table needs the previous status on UPDATE events, which pgoutput
-- only sends with REPLICA IDENTITY FULL. Limited to loans because it puts the
-- whole old row in the WAL for every update.
ALTER TABLE loans REPLICA IDENTITY FULL;

CREATE PUBLICATION lakehouse_cdc FOR TABLE
    borrowers, loans, payments, loan_status_history, pipeline_marker;
