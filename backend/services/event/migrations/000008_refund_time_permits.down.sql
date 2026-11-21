DO $$
BEGIN
    EXECUTE 'LOCK TABLE event_refund_time_permits IN ACCESS EXCLUSIVE MODE';

    IF EXISTS (SELECT 1 FROM event_refund_time_permits) THEN
        RAISE EXCEPTION 'cannot roll back Event refund time migration while permit evidence exists'
            USING ERRCODE = '55000';
    END IF;

    EXECUTE 'DROP TRIGGER IF EXISTS events_refund_time_permit_guard ON events';
    EXECUTE 'DROP FUNCTION IF EXISTS guard_event_refund_time_permits()';
    EXECUTE 'DROP TRIGGER IF EXISTS event_refund_time_permits_immutable ON event_refund_time_permits';
    EXECUTE 'DROP FUNCTION IF EXISTS enforce_event_refund_time_permit_immutability()';
    EXECUTE 'DROP TABLE event_refund_time_permits';
    EXECUTE 'ALTER TABLE events DROP COLUMN date_version';
END;
$$ LANGUAGE plpgsql;
