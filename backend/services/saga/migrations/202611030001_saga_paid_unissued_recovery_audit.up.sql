CREATE TABLE saga_paid_unissued_recovery_audit (
    id UUID PRIMARY KEY,
    operation_key UUID NOT NULL,
    operation_mode TEXT NOT NULL CHECK (operation_mode IN ('dry_run', 'execute')),
    saga_id UUID NOT NULL REFERENCES sagas(id) ON DELETE RESTRICT,
    booking_id UUID NOT NULL,
    operator_identity TEXT NOT NULL CHECK (length(btrim(operator_identity)) BETWEEN 1 AND 128),
    reason TEXT NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 500),
    expected_provider_account TEXT NOT NULL CHECK (length(btrim(expected_provider_account)) BETWEEN 1 AND 128),
    payment_id UUID,
    result_code TEXT NOT NULL CHECK (length(result_code) BETWEEN 1 AND 96),
    eligible BOOLEAN NOT NULL,
    requeued BOOLEAN NOT NULL DEFAULT FALSE,
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    state_before JSONB NOT NULL CHECK (jsonb_typeof(state_before) = 'object'),
    state_after JSONB NOT NULL CHECK (jsonb_typeof(state_after) = 'object'),
    authority_snapshot JSONB NOT NULL CHECK (jsonb_typeof(authority_snapshot) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT saga_paid_unissued_recovery_key_mode UNIQUE (operation_key, operation_mode)
);

CREATE INDEX saga_paid_unissued_recovery_audit_booking
    ON saga_paid_unissued_recovery_audit (booking_id, created_at DESC);
