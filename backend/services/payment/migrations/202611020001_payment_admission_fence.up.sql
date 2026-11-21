CREATE TABLE payment_admission_mode (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    activated_at TIMESTAMPTZ,
    CHECK ((active AND activated_at IS NOT NULL) OR (NOT active AND activated_at IS NULL))
);

INSERT INTO payment_admission_mode(singleton, active, activated_at) VALUES (TRUE, FALSE, NULL);

COMMENT ON TABLE payment_admission_mode IS
    'Monotonic Payment admission activation latch. Activate only after Booking guard activation and upgraded Booking/Payment binaries; SQL: UPDATE payment_admission_mode SET active=TRUE WHERE singleton=TRUE. This latch cannot be reset.';

CREATE FUNCTION payment_admission_mode_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'payment admission mode latch cannot be deleted';
    END IF;
    IF OLD.active AND NOT NEW.active THEN
        RAISE EXCEPTION 'payment admission mode cannot be deactivated';
    END IF;
    IF OLD.active THEN
        IF NEW.activated_at IS DISTINCT FROM OLD.activated_at THEN
            RAISE EXCEPTION 'payment admission activation timestamp is immutable';
        END IF;
    ELSIF NEW.active THEN
        NEW.activated_at := clock_timestamp();
    ELSE
        NEW.activated_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payment_admission_mode_latch
    BEFORE UPDATE OR DELETE ON payment_admission_mode
    FOR EACH ROW EXECUTE FUNCTION payment_admission_mode_monotonic();

CREATE FUNCTION payment_admission_no_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'payment admission coordination history cannot be truncated' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER payment_admission_mode_no_truncate
    BEFORE TRUNCATE ON payment_admission_mode
    FOR EACH STATEMENT EXECUTE FUNCTION payment_admission_no_truncate();

CREATE SEQUENCE payment_admission_fence_seq AS BIGINT MINVALUE 1 NO CYCLE;

CREATE TABLE payment_admission_permits (
    request_id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(id) ON DELETE RESTRICT,
    booking_id UUID NOT NULL,
    ticket_id UUID NOT NULL,
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    key_digest BYTEA NOT NULL CHECK (octet_length(key_digest) = 32),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    identity_digest BYTEA NOT NULL CHECK (octet_length(identity_digest) = 32),
    token UUID NOT NULL UNIQUE,
    fence BIGINT NOT NULL UNIQUE CHECK (fence > 0),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'ABORTED')),
    evidence_source TEXT NOT NULL CHECK (length(evidence_source) BETWEEN 1 AND 64),
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '15 seconds'),
    recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (recovery_attempts >= 0),
    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 160)
);

CREATE UNIQUE INDEX payment_admission_one_pending_ticket
    ON payment_admission_permits(payment_id, ticket_id) WHERE state = 'PENDING';
CREATE INDEX payment_admission_pending_due
    ON payment_admission_permits(next_attempt_at, created_at, request_id) WHERE state = 'PENDING';
CREATE INDEX payment_admission_pending_payment
    ON payment_admission_permits(payment_id, request_id) WHERE state = 'PENDING';

CREATE FUNCTION payment_admission_permit_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'payment admission permits are immutable history';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'PENDING' OR NEW.recovery_attempts <> 0 OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'new payment admission permits must begin pending';
        END IF;
        RETURN NEW;
    END IF;

    IF (NEW.request_id, NEW.payment_id, NEW.booking_id, NEW.ticket_id, NEW.event_id, NEW.user_id, NEW.actor_id,
        NEW.generation, NEW.key_digest, NEW.fingerprint, NEW.identity_digest, NEW.token, NEW.fence,
        NEW.evidence_source, NEW.observed_at, NEW.created_at)
       IS DISTINCT FROM
       (OLD.request_id, OLD.payment_id, OLD.booking_id, OLD.ticket_id, OLD.event_id, OLD.user_id, OLD.actor_id,
        OLD.generation, OLD.key_digest, OLD.fingerprint, OLD.identity_digest, OLD.token, OLD.fence,
        OLD.evidence_source, OLD.observed_at, OLD.created_at) THEN
        RAISE EXCEPTION 'payment admission permit binding is immutable';
    END IF;
    IF OLD.state <> 'PENDING' THEN
        RAISE EXCEPTION 'terminal payment admission permit is immutable';
    END IF;
    IF NEW.state NOT IN ('PENDING', 'COMMITTED', 'ABORTED') OR NEW.recovery_attempts < OLD.recovery_attempts THEN
        RAISE EXCEPTION 'invalid payment admission permit transition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payment_admission_permit_immutable
    BEFORE INSERT OR UPDATE OR DELETE ON payment_admission_permits
    FOR EACH ROW EXECUTE FUNCTION payment_admission_permit_guard();

CREATE TRIGGER payment_admission_permit_no_truncate
    BEFORE TRUNCATE ON payment_admission_permits
    FOR EACH STATEMENT EXECUTE FUNCTION payment_admission_no_truncate();

CREATE FUNCTION payment_refund_audit_admission_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    admission_active BOOLEAN;
    locked_payment UUID;
BEGIN
    IF NEW.record_kind <> 'PREPARED' THEN
        RETURN NEW;
    END IF;

    SELECT active INTO admission_active
      FROM payment_admission_mode
     WHERE singleton = TRUE
     FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'payment admission mode latch is missing';
    END IF;
    IF NOT admission_active THEN
        RETURN NEW;
    END IF;

    SELECT id INTO locked_payment FROM payments WHERE id = NEW.payment_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'PREPARED refund has no Payment row';
    END IF;
    IF EXISTS (
        SELECT 1 FROM payment_admission_permits
         WHERE payment_id = NEW.payment_id AND state = 'PENDING'
    ) THEN
        RAISE EXCEPTION 'refund preparation waits for pending payment admission';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payment_refund_prepared_admission_guard
    BEFORE INSERT ON payment_refund_audit
    FOR EACH ROW EXECUTE FUNCTION payment_refund_audit_admission_guard();
