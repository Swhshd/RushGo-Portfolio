CREATE TABLE event_refund_approvers (
    event_id UUID NOT NULL,
    staff_user_id UUID NOT NULL,
    active BOOLEAN NOT NULL,
    granted_by UUID NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL,
    revoked_by UUID,
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (event_id, staff_user_id),
    CHECK ((active AND revoked_by IS NULL AND revoked_at IS NULL)
        OR (NOT active AND revoked_by IS NOT NULL AND revoked_at IS NOT NULL))
);
CREATE INDEX event_refund_approvers_active_staff ON event_refund_approvers(staff_user_id,event_id) WHERE active;

CREATE TABLE event_refund_approver_audit (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    staff_user_id UUID NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('granted','revoked')),
    actor_user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX event_refund_approver_audit_event_time ON event_refund_approver_audit(event_id,created_at DESC);

CREATE TABLE booking_refund_requests (
    request_id UUID PRIMARY KEY,
    booking_id UUID NOT NULL REFERENCES bookings(id) ON DELETE RESTRICT,
    event_id UUID NOT NULL,
    buyer_user_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency TEXT NOT NULL CHECK (currency = 'usd'),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
    status TEXT NOT NULL CHECK (status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION','WITHDRAWN','REJECTED','APPROVED','REFUND_PROCESSING','REFUNDED','NEEDS_ATTENTION')),
    decision_reason TEXT NOT NULL DEFAULT '',
    decision_id UUID,
    approver_user_id UUID,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    recovery_checked_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    decided_at TIMESTAMPTZ,
    payment_refund_state TEXT NOT NULL DEFAULT 'unknown',
    payment_refund_amount_cents BIGINT NOT NULL DEFAULT 0 CHECK (payment_refund_amount_cents >= 0),
    payment_refund_status TEXT NOT NULL DEFAULT '',
    ticket_revocation_state TEXT NOT NULL DEFAULT 'not_started' CHECK (ticket_revocation_state IN ('not_started','pending_payment','revoked','needs_attention')),
    ticket_revoked_at TIMESTAMPTZ,
    CHECK ((status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION','WITHDRAWN') AND decision_id IS NULL AND approver_user_id IS NULL AND decided_at IS NULL)
        OR (status IN ('REJECTED','APPROVED','REFUND_PROCESSING','REFUNDED','NEEDS_ATTENTION') AND decision_id IS NOT NULL AND approver_user_id IS NOT NULL AND decided_at IS NOT NULL))
);
CREATE INDEX booking_refund_requests_event_time ON booking_refund_requests(event_id,requested_at DESC,request_id DESC);
CREATE INDEX booking_refund_requests_booking_time ON booking_refund_requests(booking_id,requested_at DESC,request_id DESC);
CREATE INDEX booking_refund_requests_recovery ON booking_refund_requests(recovery_checked_at,requested_at,request_id)
    WHERE status IN ('APPROVED','REFUND_PROCESSING','NEEDS_ATTENTION')
       OR (status='REFUNDED' AND ticket_revocation_state<>'revoked');
CREATE UNIQUE INDEX booking_refund_one_open_request ON booking_refund_requests(booking_id)
    WHERE status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION');

CREATE TABLE booking_refund_decisions (
    decision_id UUID PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE REFERENCES booking_refund_requests(request_id) ON DELETE RESTRICT,
    booking_id UUID NOT NULL REFERENCES bookings(id) ON DELETE RESTRICT,
    event_id UUID NOT NULL,
    buyer_user_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    decision TEXT NOT NULL CHECK (decision IN ('APPROVED','REJECTED')),
    decision_reason TEXT NOT NULL DEFAULT '',
    approver_user_id UUID NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency TEXT NOT NULL CHECK (currency = 'usd'),
    payment_intent_id TEXT,
    provider_account TEXT,
    livemode BOOLEAN,
    event_starts_at TIMESTAMPTZ,
    event_authority_version TEXT NOT NULL DEFAULT '',
    decided_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((decision='APPROVED' AND payment_intent_id IS NOT NULL AND length(payment_intent_id)>0
            AND provider_account IS NOT NULL AND length(provider_account)>0 AND livemode IS NOT NULL AND event_starts_at IS NOT NULL)
        OR (decision='REJECTED' AND payment_intent_id IS NULL AND provider_account IS NULL AND livemode IS NULL AND event_starts_at IS NULL))
);
CREATE INDEX booking_refund_decisions_booking_time ON booking_refund_decisions(booking_id,decided_at DESC);

ALTER TABLE booking_refund_requests
    ADD CONSTRAINT booking_refund_request_decision_fk
    FOREIGN KEY (decision_id) REFERENCES booking_refund_decisions(decision_id) ON DELETE RESTRICT;

ALTER TABLE bookings
    ADD COLUMN refund_decision_id UUID REFERENCES booking_refund_decisions(decision_id) ON DELETE RESTRICT;
CREATE UNIQUE INDEX bookings_refund_decision_id ON bookings(refund_decision_id) WHERE refund_decision_id IS NOT NULL;

CREATE TABLE booking_refund_idempotency (
    actor_user_id UUID NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('request','withdraw','decide')),
    key_digest BYTEA NOT NULL CHECK (octet_length(key_digest)=32),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint)=32),
    request_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (actor_user_id,operation,key_digest)
);

CREATE FUNCTION booking_refund_decision_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Booking refund decisions are immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER booking_refund_decisions_immutable_trg
    BEFORE UPDATE OR DELETE ON booking_refund_decisions
    FOR EACH ROW EXECUTE FUNCTION booking_refund_decision_immutable();
CREATE TRIGGER booking_refund_decisions_no_truncate_trg
    BEFORE TRUNCATE ON booking_refund_decisions
    FOR EACH STATEMENT EXECUTE FUNCTION booking_refund_decision_immutable();

CREATE FUNCTION booking_refund_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Booking refund audit/idempotency rows are append-only' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER booking_refund_approver_audit_immutable_trg
    BEFORE UPDATE OR DELETE ON event_refund_approver_audit
    FOR EACH ROW EXECUTE FUNCTION booking_refund_append_only();
CREATE TRIGGER booking_refund_approver_audit_no_truncate_trg
    BEFORE TRUNCATE ON event_refund_approver_audit
    FOR EACH STATEMENT EXECUTE FUNCTION booking_refund_append_only();
CREATE TRIGGER booking_refund_idempotency_immutable_trg
    BEFORE UPDATE OR DELETE ON booking_refund_idempotency
    FOR EACH ROW EXECUTE FUNCTION booking_refund_append_only();
CREATE TRIGGER booking_refund_idempotency_no_truncate_trg
    BEFORE TRUNCATE ON booking_refund_idempotency
    FOR EACH STATEMENT EXECUTE FUNCTION booking_refund_append_only();

CREATE FUNCTION booking_refund_request_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Booking refund requests are durable' USING ERRCODE = '23514';
    END IF;
    IF (NEW.request_id,NEW.booking_id,NEW.event_id,NEW.buyer_user_id,NEW.payment_id,NEW.generation,NEW.amount_cents,NEW.currency,NEW.reason,NEW.requested_at)
       IS DISTINCT FROM
       (OLD.request_id,OLD.booking_id,OLD.event_id,OLD.buyer_user_id,OLD.payment_id,OLD.generation,OLD.amount_cents,OLD.currency,OLD.reason,OLD.requested_at) THEN
        RAISE EXCEPTION 'Booking refund request identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.decision_id IS NOT NULL AND
       (NEW.decision_id,NEW.approver_user_id,NEW.decided_at,NEW.decision_reason)
       IS DISTINCT FROM
       (OLD.decision_id,OLD.approver_user_id,OLD.decided_at,OLD.decision_reason) THEN
        RAISE EXCEPTION 'Booking refund decision binding is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.decision_id IS NULL AND NEW.decision_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM booking_refund_decisions d
        WHERE d.decision_id=NEW.decision_id AND d.request_id=NEW.request_id
          AND d.booking_id=NEW.booking_id AND d.event_id=NEW.event_id
          AND d.buyer_user_id=NEW.buyer_user_id AND d.payment_id=NEW.payment_id
          AND d.generation=NEW.generation AND d.approver_user_id=NEW.approver_user_id
          AND d.decision_reason=NEW.decision_reason
    ) THEN
        RAISE EXCEPTION 'Booking refund request decision does not match immutable decision record' USING ERRCODE = '23514';
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
        (OLD.status IN ('REQUESTED','WAITING_CHECKIN_RESOLUTION') AND NEW.status IN ('WAITING_CHECKIN_RESOLUTION','WITHDRAWN','REJECTED','APPROVED'))
        OR (OLD.status='APPROVED' AND NEW.status IN ('REFUND_PROCESSING','REFUNDED','NEEDS_ATTENTION'))
        OR (OLD.status='REFUND_PROCESSING' AND NEW.status IN ('REFUND_PROCESSING','REFUNDED','NEEDS_ATTENTION'))
        OR (OLD.status='NEEDS_ATTENTION' AND NEW.status IN ('NEEDS_ATTENTION','REFUND_PROCESSING','REFUNDED'))
    ) THEN
        RAISE EXCEPTION 'Booking refund request status transition is invalid' USING ERRCODE = '23514';
    END IF;
    IF OLD.status IN ('WITHDRAWN','REJECTED','REFUNDED') AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'terminal Booking refund request status is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER booking_refund_request_guard_trg
    BEFORE UPDATE OR DELETE ON booking_refund_requests
    FOR EACH ROW EXECUTE FUNCTION booking_refund_request_guard();
CREATE TRIGGER booking_refund_request_no_truncate_trg
    BEFORE TRUNCATE ON booking_refund_requests
    FOR EACH STATEMENT EXECUTE FUNCTION booking_refund_request_guard();

CREATE FUNCTION booking_refund_block_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.refund_decision_id IS NOT NULL AND NEW.refund_decision_id IS DISTINCT FROM OLD.refund_decision_id THEN
        RAISE EXCEPTION 'Booking refund decision block is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER booking_refund_block_immutable_trg
    BEFORE UPDATE OF refund_decision_id ON bookings
    FOR EACH ROW EXECUTE FUNCTION booking_refund_block_immutable();

-- The trigger's row lock serializes legacy Ticket writes with the durable Booking block.
-- Approval never locks Ticket rows, so an old Ticket->Booking wait has no cycle.
CREATE OR REPLACE FUNCTION booking_guard_ticket_checkin_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    mode_active BOOLEAN;
    intent_found BOOLEAN;
    refund_decision UUID;
BEGIN
    IF OLD.status <> 'ISSUED' OR NEW.status <> 'CHECKED_IN' THEN
        RETURN NEW;
    END IF;

    SELECT refund_decision_id INTO refund_decision
    FROM bookings
    WHERE id = NEW.booking_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Booking is unavailable for Ticket check-in' USING ERRCODE = '23514';
    END IF;
    IF refund_decision IS NOT NULL THEN
        RAISE EXCEPTION 'approved Booking refund blocks Ticket check-in' USING ERRCODE = '23514';
    END IF;

    SELECT active INTO mode_active
    FROM booking_admission_mode
    WHERE singleton = TRUE
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Booking admission mode is unavailable' USING ERRCODE = '55000';
    END IF;

    IF NOT mode_active THEN
        RETURN NEW;
    END IF;

    IF OLD.admission_request_id IS NOT NULL OR NEW.admission_request_id IS NULL THEN
        RAISE EXCEPTION 'active Booking admission requires a new request link' USING ERRCODE = '23514';
    END IF;

    SELECT TRUE INTO intent_found
    FROM ticket_admission_intents i
    JOIN bookings b ON b.id = i.booking_id
    WHERE i.request_id = NEW.admission_request_id
      AND i.ticket_id = NEW.id
      AND i.booking_id = NEW.booking_id
      AND i.payment_id = NEW.payment_id
      AND i.event_id = NEW.event_id
      AND i.user_id = b.user_id
      AND i.event_id = b.event_id
      AND i.generation = NEW.lifecycle_generation
      AND i.state = 'PENDING'
      AND i.permit_token IS NOT NULL
      AND i.permit_fence > 0
    FOR UPDATE OF i;

    IF NOT COALESCE(intent_found, FALSE) THEN
        RAISE EXCEPTION 'active Booking admission requires a matching pending permit' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
