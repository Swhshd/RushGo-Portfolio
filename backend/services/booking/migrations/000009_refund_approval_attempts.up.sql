CREATE TABLE booking_refund_approval_attempts (
    attempt_id UUID PRIMARY KEY,
    request_id UUID NOT NULL REFERENCES booking_refund_requests(request_id) ON DELETE RESTRICT,
    booking_id UUID NOT NULL REFERENCES bookings(id) ON DELETE RESTRICT,
    event_id UUID NOT NULL,
    buyer_user_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    approver_user_id UUID NOT NULL,
    decision_key_digest BYTEA NOT NULL CHECK (octet_length(decision_key_digest) = 32),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    reason TEXT NOT NULL CHECK (length(reason) <= 500),
    state TEXT NOT NULL CHECK (state IN ('PENDING','APPROVED','REJECTED','ABORTED')),
    event_starts_at TIMESTAMPTZ,
    event_qualified_at TIMESTAMPTZ,
    event_authority_version TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    recovery_checked_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    terminal_at TIMESTAMPTZ,
    release_acknowledged_at TIMESTAMPTZ,
    CHECK ((state = 'PENDING' AND terminal_at IS NULL AND release_acknowledged_at IS NULL)
        OR (state <> 'PENDING' AND terminal_at IS NOT NULL)),
    CHECK ((event_starts_at IS NULL AND event_qualified_at IS NULL)
        OR (event_starts_at IS NOT NULL AND event_qualified_at IS NOT NULL AND event_qualified_at < event_starts_at)),
    CHECK (state <> 'APPROVED' OR (event_starts_at IS NOT NULL AND event_qualified_at IS NOT NULL AND event_authority_version <> ''))
);
CREATE UNIQUE INDEX booking_refund_one_approval_attempt_per_request ON booking_refund_approval_attempts(request_id);
CREATE INDEX booking_refund_approval_attempt_recovery ON booking_refund_approval_attempts(recovery_checked_at,created_at,attempt_id)
    WHERE state = 'PENDING' OR release_acknowledged_at IS NULL;

ALTER TABLE booking_refund_requests
    ADD COLUMN qualification_attempt_id UUID,
    ADD COLUMN event_qualified_at TIMESTAMPTZ,
    ADD CONSTRAINT booking_refund_request_qualification_attempt_fk
        FOREIGN KEY (qualification_attempt_id) REFERENCES booking_refund_approval_attempts(attempt_id) ON DELETE RESTRICT,
    ADD CONSTRAINT booking_refund_request_qualification_pair_check
        CHECK ((qualification_attempt_id IS NULL AND event_qualified_at IS NULL)
            OR (qualification_attempt_id IS NOT NULL));

ALTER TABLE booking_refund_decisions
    ADD COLUMN qualification_attempt_id UUID,
    ADD COLUMN event_qualified_at TIMESTAMPTZ,
    ADD CONSTRAINT booking_refund_decision_qualification_attempt_fk
        FOREIGN KEY (qualification_attempt_id) REFERENCES booking_refund_approval_attempts(attempt_id) ON DELETE RESTRICT,
    ADD CONSTRAINT booking_refund_decision_qualification_pair_check
        CHECK ((qualification_attempt_id IS NULL AND event_qualified_at IS NULL)
            OR (decision = 'APPROVED' AND qualification_attempt_id IS NOT NULL AND event_qualified_at IS NOT NULL
                AND event_starts_at IS NOT NULL AND event_qualified_at < event_starts_at));

CREATE FUNCTION booking_refund_decision_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.qualification_attempt_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM booking_refund_approval_attempts a
        WHERE a.attempt_id=NEW.qualification_attempt_id AND a.attempt_id=NEW.decision_id
          AND a.request_id=NEW.request_id AND a.booking_id=NEW.booking_id AND a.event_id=NEW.event_id
          AND a.buyer_user_id=NEW.buyer_user_id AND a.payment_id=NEW.payment_id AND a.generation=NEW.generation
          AND a.approver_user_id=NEW.approver_user_id AND a.state='APPROVED'
          AND a.event_starts_at=NEW.event_starts_at AND a.event_qualified_at=NEW.event_qualified_at
          AND a.event_authority_version=NEW.event_authority_version
    ) THEN
        RAISE EXCEPTION 'Booking refund decision does not match qualified approval attempt' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER booking_refund_decision_attempt_guard_trg
    BEFORE INSERT ON booking_refund_decisions
    FOR EACH ROW EXECUTE FUNCTION booking_refund_decision_attempt_guard();

CREATE FUNCTION booking_refund_approval_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Booking refund approval attempts are durable' USING ERRCODE = '23514';
    END IF;
    IF (NEW.attempt_id,NEW.request_id,NEW.booking_id,NEW.event_id,NEW.buyer_user_id,NEW.payment_id,NEW.generation,
        NEW.approver_user_id,NEW.decision_key_digest,NEW.request_fingerprint,NEW.reason,NEW.created_at)
       IS DISTINCT FROM
       (OLD.attempt_id,OLD.request_id,OLD.booking_id,OLD.event_id,OLD.buyer_user_id,OLD.payment_id,OLD.generation,
        OLD.approver_user_id,OLD.decision_key_digest,OLD.request_fingerprint,OLD.reason,OLD.created_at) THEN
        RAISE EXCEPTION 'Booking refund approval attempt binding is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.state <> 'PENDING' AND
       (NEW.state,NEW.event_starts_at,NEW.event_qualified_at,NEW.event_authority_version,NEW.terminal_at)
       IS DISTINCT FROM
       (OLD.state,OLD.event_starts_at,OLD.event_qualified_at,OLD.event_authority_version,OLD.terminal_at) THEN
        RAISE EXCEPTION 'terminal Booking refund approval attempt is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.state = 'PENDING' AND NEW.state NOT IN ('PENDING','APPROVED','REJECTED','ABORTED') THEN
        RAISE EXCEPTION 'Booking refund approval attempt transition is invalid' USING ERRCODE = '23514';
    END IF;
    IF OLD.state = 'PENDING' AND NEW.state <> 'PENDING' AND NEW.terminal_at IS NULL THEN
        RAISE EXCEPTION 'terminal Booking refund approval attempt requires terminal time' USING ERRCODE = '23514';
    END IF;
    IF OLD.release_acknowledged_at IS NOT NULL AND NEW.release_acknowledged_at IS DISTINCT FROM OLD.release_acknowledged_at THEN
        RAISE EXCEPTION 'Booking refund approval attempt release acknowledgement is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.release_acknowledged_at IS NOT NULL AND NEW.state = 'PENDING' THEN
        RAISE EXCEPTION 'pending Booking refund approval attempt cannot be released' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER booking_refund_approval_attempt_guard_trg
    BEFORE UPDATE OR DELETE ON booking_refund_approval_attempts
    FOR EACH ROW EXECUTE FUNCTION booking_refund_approval_attempt_guard();
CREATE TRIGGER booking_refund_approval_attempt_no_truncate_trg
    BEFORE TRUNCATE ON booking_refund_approval_attempts
    FOR EACH STATEMENT EXECUTE FUNCTION booking_refund_approval_attempt_guard();

CREATE FUNCTION booking_refund_request_attempt_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.qualification_attempt_id IS DISTINCT FROM OLD.qualification_attempt_id AND NEW.qualification_attempt_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM booking_refund_approval_attempts a
           WHERE a.attempt_id=NEW.qualification_attempt_id AND a.request_id=NEW.request_id
             AND a.booking_id=NEW.booking_id AND a.event_id=NEW.event_id AND a.buyer_user_id=NEW.buyer_user_id
             AND a.payment_id=NEW.payment_id AND a.generation=NEW.generation
             AND (a.state='PENDING' OR (a.state='APPROVED' AND a.event_qualified_at=NEW.event_qualified_at))
       ) THEN
        RAISE EXCEPTION 'Booking refund request qualification attempt does not match durable attempt' USING ERRCODE = '23514';
    END IF;
    IF NEW.event_qualified_at IS DISTINCT FROM OLD.event_qualified_at AND NEW.event_qualified_at IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM booking_refund_approval_attempts a
           WHERE a.attempt_id=NEW.qualification_attempt_id AND a.request_id=NEW.request_id
             AND a.state='APPROVED' AND a.event_qualified_at=NEW.event_qualified_at
       ) THEN
        RAISE EXCEPTION 'Booking refund request qualification time does not match approved attempt' USING ERRCODE = '23514';
    END IF;
    IF OLD.decision_id IS NOT NULL AND
       (NEW.qualification_attempt_id,NEW.event_qualified_at) IS DISTINCT FROM (OLD.qualification_attempt_id,OLD.event_qualified_at) THEN
        RAISE EXCEPTION 'Booking refund request qualification proof is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER booking_refund_request_attempt_guard_trg
    BEFORE UPDATE ON booking_refund_requests
    FOR EACH ROW EXECUTE FUNCTION booking_refund_request_attempt_guard();
