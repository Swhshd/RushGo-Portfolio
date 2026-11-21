ALTER TABLE events
    ADD COLUMN date_version BIGINT NOT NULL DEFAULT 1 CHECK (date_version > 0);

CREATE TABLE event_refund_time_permits (
    attempt_id UUID PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE,
    booking_id UUID NOT NULL,
    payment_id UUID NOT NULL,
    event_id UUID NOT NULL,
    approver_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL CHECK (state IN ('QUALIFIED', 'REJECTED', 'RELEASED')),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('APPROVED', 'REJECTED', 'ABORTED')),
    starts_at TIMESTAMPTZ,
    qualified_at TIMESTAMPTZ,
    event_date_version BIGINT CHECK (event_date_version IS NULL OR event_date_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT event_refund_time_permits_state_shape CHECK (
        (state = 'QUALIFIED' AND outcome IS NULL AND starts_at IS NOT NULL AND qualified_at IS NOT NULL
            AND qualified_at < starts_at AND event_date_version IS NOT NULL)
        OR
        (state = 'REJECTED' AND outcome IS NULL AND starts_at IS NOT NULL AND qualified_at IS NOT NULL
            AND qualified_at >= starts_at AND event_date_version IS NOT NULL)
        OR
        (state = 'RELEASED' AND outcome IS NOT NULL AND
            ((starts_at IS NULL AND qualified_at IS NULL AND event_date_version IS NULL)
                OR (starts_at IS NOT NULL AND qualified_at IS NOT NULL AND event_date_version IS NOT NULL)))
    )
);

CREATE INDEX idx_event_refund_time_permits_active_event
    ON event_refund_time_permits(event_id)
    WHERE state = 'QUALIFIED';

CREATE FUNCTION enforce_event_refund_time_permit_immutability() RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.attempt_id, NEW.request_id, NEW.booking_id, NEW.payment_id, NEW.event_id,
           NEW.approver_id, NEW.generation, NEW.fingerprint, NEW.starts_at, NEW.qualified_at,
           NEW.event_date_version, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.attempt_id, OLD.request_id, OLD.booking_id, OLD.payment_id, OLD.event_id,
           OLD.approver_id, OLD.generation, OLD.fingerprint, OLD.starts_at, OLD.qualified_at,
           OLD.event_date_version, OLD.created_at) THEN
        RAISE EXCEPTION 'refund time permit binding and qualification evidence are immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_immutable';
    END IF;

    IF OLD.state = 'RELEASED' AND (NEW.state IS DISTINCT FROM OLD.state OR NEW.outcome IS DISTINCT FROM OLD.outcome) THEN
        RAISE EXCEPTION 'refund time permit release is terminal'
            USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_immutable';
    END IF;
    IF OLD.state IN ('QUALIFIED', 'REJECTED') AND NEW.state NOT IN (OLD.state, 'RELEASED') THEN
        RAISE EXCEPTION 'refund time permit state transition is not allowed'
            USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_immutable';
    END IF;
    IF OLD.state = NEW.state AND NEW.outcome IS DISTINCT FROM OLD.outcome THEN
        RAISE EXCEPTION 'refund time permit outcome is immutable before release'
            USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_immutable';
    END IF;
    IF OLD.state = 'REJECTED' AND NEW.state = 'RELEASED' AND NEW.outcome = 'APPROVED' THEN
        RAISE EXCEPTION 'a rejected time qualification cannot be approved'
            USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER event_refund_time_permits_immutable
    BEFORE UPDATE ON event_refund_time_permits
    FOR EACH ROW EXECUTE FUNCTION enforce_event_refund_time_permit_immutability();

CREATE FUNCTION guard_event_refund_time_permits() RETURNS trigger AS $$
DECLARE
    active_permit BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        SELECT EXISTS (
            SELECT 1 FROM event_refund_time_permits
            WHERE event_id = OLD.id AND state = 'QUALIFIED'
        ) INTO active_permit;
        IF active_permit THEN
            RAISE EXCEPTION 'qualified refund time permit blocks Event deletion'
                USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_guard';
        END IF;
        RETURN OLD;
    END IF;

    IF NEW.date IS DISTINCT FROM OLD.date THEN
        SELECT EXISTS (
            SELECT 1 FROM event_refund_time_permits
            WHERE event_id = OLD.id AND state = 'QUALIFIED'
        ) INTO active_permit;
        IF active_permit THEN
            RAISE EXCEPTION 'qualified refund time permit blocks Event date change'
                USING ERRCODE = '23514', CONSTRAINT = 'event_refund_time_permit_guard';
        END IF;
        NEW.date_version := OLD.date_version + 1;
    ELSE
        NEW.date_version := OLD.date_version;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_refund_time_permit_guard
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION guard_event_refund_time_permits();
