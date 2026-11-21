CREATE TABLE payment_provider_events (
    id UUID PRIMARY KEY,
    provider_account TEXT NOT NULL CHECK (length(provider_account)>0),
    livemode BOOLEAN NOT NULL,
    event_id TEXT NOT NULL CHECK (length(event_id)>0),
    event_type TEXT NOT NULL,
    intent_id TEXT,
    refund_id TEXT,
    payment_id UUID,
    booking_id UUID,
    payment_generation BIGINT,
    normalized_object JSONB NOT NULL,
    evidence_hash BYTEA NOT NULL CHECK (octet_length(evidence_hash)=32),
    amount_cents BIGINT NOT NULL,
    currency TEXT NOT NULL,
    provider_status TEXT NOT NULL,
    collected BOOLEAN NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    handled_at TIMESTAMPTZ,
    correlation_state TEXT NOT NULL CHECK (correlation_state IN ('pending','applied','attention')),
    relay_state TEXT NOT NULL CHECK (relay_state IN ('pending','claimed','published','not_applicable')),
    envelope_id UUID NOT NULL,
    claim_token UUID,
    claim_fence BIGINT NOT NULL DEFAULT 0 CHECK (claim_fence>=0),
    lease_until TIMESTAMPTZ,
    due_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts>=0),
    last_error TEXT,
    UNIQUE(provider_account,livemode,event_id),
    CHECK (correlation_state<>'applied' OR (payment_id IS NOT NULL AND booking_id IS NOT NULL AND payment_generation=1))
);
CREATE INDEX payment_provider_events_pending_correlation ON payment_provider_events(due_at,id)
    WHERE correlation_state='pending';
CREATE INDEX payment_provider_events_pending_relay ON payment_provider_events(due_at,id)
    WHERE correlation_state='applied' AND relay_state IN ('pending','claimed');
