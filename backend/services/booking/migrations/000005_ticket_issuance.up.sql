CREATE TABLE ticket_issuance_receipts (
    event_id UUID PRIMARY KEY,
    saga_id UUID NOT NULL,
    booking_id UUID NOT NULL REFERENCES bookings(id),
    payment_id UUID NOT NULL,
    event_ref_id UUID NOT NULL,
    lifecycle_generation BIGINT NOT NULL CHECK (lifecycle_generation = 1),
    outcome TEXT NOT NULL CHECK (outcome IN ('issued','denied')),
    reason_code TEXT NOT NULL,
    dispute_state TEXT NOT NULL CHECK (dispute_state IN ('unknown','provider_observed_no_dispute','dispute_present')),
    refund_state TEXT NOT NULL CHECK (refund_state IN ('unknown','provider_observed_no_refund','refund_present')),
    evidence_source TEXT NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ,
    ticket_count INTEGER NOT NULL DEFAULT 0 CHECK (ticket_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (saga_id, booking_id, lifecycle_generation),
    CHECK ((outcome='issued' AND ticket_count>0 AND observed_at IS NOT NULL AND evidence_source='stripe_test_api'
            AND dispute_state='provider_observed_no_dispute' AND refund_state='provider_observed_no_refund')
        OR (outcome='denied' AND ticket_count=0))
);

CREATE TABLE tickets (
    id UUID PRIMARY KEY,
    booking_id UUID NOT NULL REFERENCES bookings(id),
    booking_item_id UUID NOT NULL REFERENCES booking_items(id),
    saga_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    event_id UUID NOT NULL,
    ticket_tier_id UUID NOT NULL,
    seat_id UUID NOT NULL,
    lifecycle_generation BIGINT NOT NULL CHECK (lifecycle_generation=1),
    status TEXT NOT NULL CHECK (status IN ('ISSUED','CHECKED_IN','REVOKED')) DEFAULT 'ISSUED',
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    checked_in_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    UNIQUE (booking_item_id, seat_id),
    UNIQUE (event_id, seat_id)
);
CREATE INDEX tickets_booking_id_id ON tickets(booking_id,id);
CREATE INDEX tickets_event_id_status ON tickets(event_id,status);

CREATE TABLE ticket_credentials (
    ticket_id UUID PRIMARY KEY REFERENCES tickets(id),
    token_digest BYTEA NOT NULL UNIQUE CHECK (octet_length(token_digest)=32),
    expires_at TIMESTAMPTZ NOT NULL,
    credential_version BIGINT NOT NULL CHECK (credential_version>0),
    rotated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE event_scanner_assignments (
    event_id UUID NOT NULL,
    staff_user_id UUID NOT NULL,
    active BOOLEAN NOT NULL,
    granted_by UUID NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL,
    revoked_by UUID,
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (event_id,staff_user_id),
    CHECK ((active AND revoked_by IS NULL AND revoked_at IS NULL)
        OR (NOT active AND revoked_by IS NOT NULL AND revoked_at IS NOT NULL))
);
CREATE INDEX event_scanner_active_staff ON event_scanner_assignments(staff_user_id,event_id) WHERE active;

CREATE TABLE event_scanner_assignment_audit (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    staff_user_id UUID NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('granted','revoked')),
    actor_user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX event_scanner_audit_event_time ON event_scanner_assignment_audit(event_id,created_at DESC);

CREATE TABLE ticket_checkins (
    id UUID PRIMARY KEY,
    ticket_id UUID NOT NULL UNIQUE REFERENCES tickets(id),
    event_id UUID NOT NULL,
    staff_user_id UUID NOT NULL,
    result_code TEXT NOT NULL CHECK (result_code='accepted'),
    dispute_state TEXT NOT NULL CHECK (dispute_state='provider_observed_no_dispute'),
    refund_state TEXT NOT NULL CHECK (refund_state='provider_observed_no_refund'),
    evidence_source TEXT NOT NULL CHECK (evidence_source='stripe_test_api'),
    observed_at TIMESTAMPTZ NOT NULL,
    checked_in_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ticket_checkins_event_time ON ticket_checkins(event_id,checked_in_at DESC);

CREATE TABLE ticket_scan_requests (
    staff_user_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint)=32),
    event_id UUID NOT NULL,
    ticket_id UUID REFERENCES tickets(id),
    result_code TEXT NOT NULL,
    reason_code TEXT NOT NULL DEFAULT '',
    checked_in_at TIMESTAMPTZ,
    dispute_state TEXT NOT NULL DEFAULT 'unknown' CHECK (dispute_state IN ('unknown','provider_observed_no_dispute','dispute_present')),
    refund_state TEXT NOT NULL DEFAULT 'unknown' CHECK (refund_state IN ('unknown','provider_observed_no_refund','refund_present')),
    evidence_source TEXT NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (staff_user_id,idempotency_key)
);
