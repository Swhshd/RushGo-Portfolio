ALTER TABLE payments
    ADD COLUMN managed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN payment_generation BIGINT,
    ADD COLUMN provider_account TEXT,
    ADD COLUMN livemode BOOLEAN,
    ADD COLUMN request_hash BYTEA,
    ADD COLUMN frozen_create JSONB,
    ADD COLUMN create_key TEXT,
    ADD COLUMN create_state TEXT,
    ADD COLUMN create_first_attempt_at TIMESTAMPTZ,
    ADD COLUMN provider_outcome TEXT,
    ADD COLUMN provider_status TEXT,
    ADD COLUMN provider_observed_at TIMESTAMPTZ,
    ADD COLUMN booking_disposition TEXT,
    ADD COLUMN cancel_key TEXT,
    ADD COLUMN cancel_state TEXT,
    ADD COLUMN cancel_first_attempt_at TIMESTAMPTZ,
    ADD COLUMN reconciliation_state TEXT,
    ADD COLUMN refund_state TEXT,
    ADD COLUMN refund_reason TEXT,
    ADD COLUMN refund_decision_id UUID,
    ADD COLUMN refund_amount_cents BIGINT,
    ADD COLUMN refund_currency TEXT,
    ADD COLUMN refund_key TEXT,
    ADD COLUMN refund_provider_id TEXT,
    ADD COLUMN refund_provider_status TEXT,
    ADD COLUMN refund_requested_at TIMESTAMPTZ,
    ADD COLUMN refund_first_attempt_at TIMESTAMPTZ,
    ADD COLUMN refund_completed_at TIMESTAMPTZ,
    ADD COLUMN operation_kind TEXT,
    ADD COLUMN claim_token UUID,
    ADD COLUMN claim_fence BIGINT,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN due_at TIMESTAMPTZ,
    ADD COLUMN attempts INTEGER,
    ADD COLUMN last_error TEXT;

ALTER TABLE payments ADD CONSTRAINT payments_managed_identity_check CHECK (NOT managed OR (
    booking_id IS NOT NULL AND booking_id <> '00000000-0000-0000-0000-000000000000'::uuid
    AND payment_generation IS NOT NULL AND payment_generation = 1
    AND provider_account IS NOT NULL AND length(provider_account) > 0
    AND livemode IS NOT NULL AND request_hash IS NOT NULL AND octet_length(request_hash) = 32
    AND frozen_create IS NOT NULL AND jsonb_typeof(frozen_create) = 'object'
    AND price BETWEEN 1 AND 2147483647 AND currency = 'usd'
    AND create_key IS NOT NULL AND create_key = 'ticketbox:payment:' || id::text || ':g1:create'
    AND cancel_key IS NOT NULL AND cancel_key = 'ticketbox:payment:' || id::text || ':g1:cancel'
    AND refund_key IS NOT NULL AND refund_key = 'ticketbox:payment:' || id::text || ':g1:refund'
    AND claim_fence IS NOT NULL AND claim_fence >= 0
    AND attempts IS NOT NULL AND attempts >= 0 AND due_at IS NOT NULL
    AND provider_outcome IS NOT NULL AND provider_outcome IN ('unknown','not_collected','collected')
    AND create_state IS NOT NULL AND create_state IN ('create_pending','create_claimed','create_unknown','create_ready','create_attention')
    AND cancel_state IS NOT NULL AND cancel_state IN ('not_requested','cancel_pending','cancel_claimed','cancel_unknown','cancelled','cancel_attention')
    AND reconciliation_state IS NOT NULL AND reconciliation_state IN ('pending','claimed','sale_verified','refund_required','resolved','attention')
    AND refund_state IS NOT NULL AND refund_state IN ('not_required','pending','claimed','provider_pending','retryable','succeeded','attention','unknown')
    AND (provider_outcome <> 'collected' OR (status IS NOT NULL AND status = 'success'))
));

CREATE UNIQUE INDEX payments_managed_logical_identity ON payments (booking_id, payment_generation) WHERE managed;
CREATE UNIQUE INDEX payments_managed_provider_intent ON payments (provider_account, livemode, payment_intent_id)
    WHERE managed AND payment_intent_id IS NOT NULL AND payment_intent_id <> '';
CREATE INDEX payments_managed_due_work ON payments (due_at, id) WHERE managed
    AND (create_state <> 'create_ready' OR cancel_state NOT IN ('not_requested','cancelled')
         OR reconciliation_state NOT IN ('sale_verified','resolved') OR refund_state NOT IN ('not_required','succeeded'));
