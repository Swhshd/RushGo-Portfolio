CREATE TABLE saga_ticket_issuance_outbox (
    id UUID PRIMARY KEY,
    saga_id UUID NOT NULL REFERENCES sagas(id) ON DELETE RESTRICT,
    booking_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    event_id UUID NOT NULL,
    lifecycle_generation BIGINT NOT NULL CHECK (lifecycle_generation = 1),
    event_type TEXT NOT NULL CHECK (event_type = 'TicketIssuanceRequested.v1'),
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token UUID,
    claim_until TIMESTAMPTZ,
    last_error_category TEXT,
    CONSTRAINT saga_ticket_outbox_claim_pair CHECK ((claim_token IS NULL) = (claim_until IS NULL)),
    CONSTRAINT saga_ticket_outbox_saga_unique UNIQUE (saga_id)
);

CREATE INDEX saga_ticket_issuance_outbox_due ON saga_ticket_issuance_outbox(next_attempt_at, id)
    WHERE published_at IS NULL;
