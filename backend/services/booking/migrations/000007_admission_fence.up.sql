CREATE TABLE booking_admission_mode (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    activated_at TIMESTAMPTZ,
    CONSTRAINT booking_admission_mode_activation_ck CHECK ((active AND activated_at IS NOT NULL) OR (NOT active AND activated_at IS NULL))
);

INSERT INTO booking_admission_mode(singleton, active, activated_at) VALUES (TRUE, FALSE, NULL);

CREATE FUNCTION booking_admission_mode_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.active AND NOT NEW.active THEN
        RAISE EXCEPTION 'Booking admission mode cannot be deactivated' USING ERRCODE = '23514';
    END IF;
    IF NEW.active AND NOT OLD.active THEN
        NEW.activated_at := clock_timestamp();
    ELSIF OLD.active THEN
        NEW.activated_at := OLD.activated_at;
    ELSE
        NEW.activated_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER booking_admission_mode_monotonic_trg
    BEFORE UPDATE OF active, activated_at ON booking_admission_mode
    FOR EACH ROW EXECUTE FUNCTION booking_admission_mode_monotonic();

CREATE FUNCTION booking_admission_mode_no_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Booking admission mode row cannot be removed' USING ERRCODE = '23514';
    RETURN NULL;
END;
$$;

CREATE TRIGGER booking_admission_mode_no_delete_trg
    BEFORE DELETE ON booking_admission_mode
    FOR EACH ROW EXECUTE FUNCTION booking_admission_mode_no_delete();
CREATE TRIGGER booking_admission_mode_no_truncate_trg
    BEFORE TRUNCATE ON booking_admission_mode
    FOR EACH STATEMENT EXECUTE FUNCTION booking_admission_mode_no_delete();

CREATE TABLE ticket_admission_intents (
    request_id UUID PRIMARY KEY,
    payment_id UUID NOT NULL,
    booking_id UUID NOT NULL,
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE RESTRICT,
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    key_digest BYTEA NOT NULL CHECK (octet_length(key_digest) = 32),
    request_fingerprint BYTEA NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    identity_digest BYTEA NOT NULL CHECK (octet_length(identity_digest) = 32),
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'COMMITTED', 'ABORTED')),
    permit_token UUID,
    permit_fence BIGINT,
    result_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ticket_admission_intents_key_uq UNIQUE(actor_id, key_digest),
    CONSTRAINT ticket_admission_intents_permit_ck CHECK (
        (permit_token IS NULL AND permit_fence IS NULL)
        OR (permit_token IS NOT NULL AND permit_fence IS NOT NULL AND permit_fence > 0)
    ),
    CONSTRAINT ticket_admission_intents_terminal_ck CHECK (
        (state = 'PENDING' AND result_code IS NULL)
        OR (state = 'COMMITTED' AND result_code = 'accepted' AND permit_token IS NOT NULL)
        OR (state = 'ABORTED' AND (result_code IS NULL OR result_code IN ('denied', 'already_checked_in')))
    )
);

CREATE INDEX ticket_admission_intents_payment_state_idx
    ON ticket_admission_intents(payment_id, state, updated_at);
CREATE INDEX ticket_admission_intents_booking_state_idx
    ON ticket_admission_intents(booking_id, state);

CREATE FUNCTION booking_ticket_admission_intent_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'Booking admission intents cannot be removed' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'PENDING' OR NEW.permit_token IS NOT NULL OR NEW.permit_fence IS NOT NULL OR NEW.result_code IS NOT NULL THEN
            RAISE EXCEPTION 'Booking admission intents must start pending without a permit' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.state <> 'PENDING' THEN
        RAISE EXCEPTION 'Booking admission intent terminal state is immutable' USING ERRCODE = '23514';
    END IF;
    IF (NEW.request_id,NEW.payment_id,NEW.booking_id,NEW.ticket_id,NEW.event_id,NEW.user_id,NEW.actor_id,NEW.generation,
        NEW.key_digest,NEW.request_fingerprint,NEW.identity_digest,NEW.created_at)
       IS DISTINCT FROM
       (OLD.request_id,OLD.payment_id,OLD.booking_id,OLD.ticket_id,OLD.event_id,OLD.user_id,OLD.actor_id,OLD.generation,
        OLD.key_digest,OLD.request_fingerprint,OLD.identity_digest,OLD.created_at) THEN
        RAISE EXCEPTION 'Booking admission intent identity is immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.permit_token IS NOT NULL AND (NEW.permit_token,NEW.permit_fence) IS DISTINCT FROM (OLD.permit_token,OLD.permit_fence) THEN
        RAISE EXCEPTION 'Booking admission permit binding is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER booking_ticket_admission_intent_guard_trg
    BEFORE INSERT OR UPDATE OR DELETE ON ticket_admission_intents
    FOR EACH ROW EXECUTE FUNCTION booking_ticket_admission_intent_guard();
CREATE TRIGGER booking_ticket_admission_intent_no_truncate_trg
    BEFORE TRUNCATE ON ticket_admission_intents
    FOR EACH STATEMENT EXECUTE FUNCTION booking_ticket_admission_intent_guard();

ALTER TABLE tickets ADD COLUMN admission_request_id UUID;
CREATE INDEX tickets_admission_request_idx ON tickets(admission_request_id) WHERE admission_request_id IS NOT NULL;

CREATE FUNCTION booking_lock_admission_mode_for_ticket_update() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    ignored_active BOOLEAN;
BEGIN
    SELECT active INTO ignored_active
    FROM booking_admission_mode
    WHERE singleton = TRUE
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Booking admission mode is unavailable' USING ERRCODE = '55000';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER booking_lock_admission_mode_for_ticket_update_trg
    BEFORE UPDATE ON tickets
    FOR EACH STATEMENT EXECUTE FUNCTION booking_lock_admission_mode_for_ticket_update();

CREATE FUNCTION booking_guard_ticket_checkin_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    mode_active BOOLEAN;
    intent_found BOOLEAN;
BEGIN
    IF OLD.status <> 'ISSUED' OR NEW.status <> 'CHECKED_IN' THEN
        RETURN NEW;
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

CREATE TRIGGER booking_guard_ticket_checkin_admission_trg
    BEFORE UPDATE OF status ON tickets
    FOR EACH ROW EXECUTE FUNCTION booking_guard_ticket_checkin_admission();
