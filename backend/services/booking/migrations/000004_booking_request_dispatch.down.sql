DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM bookings WHERE request_id IS NOT NULL OR dispatch_state IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot remove retained Booking request or dispatch responsibility';
    END IF;
END $$;
DROP INDEX bookings_dispatch_due;
DROP INDEX bookings_user_request_identity;
ALTER TABLE bookings DROP CONSTRAINT bookings_dispatch_shape, DROP CONSTRAINT bookings_request_identity,
    DROP COLUMN request_id,DROP COLUMN request_hash,DROP COLUMN dispatch_kind,DROP COLUMN dispatch_state,
    DROP COLUMN dispatch_token,DROP COLUMN dispatch_fence,DROP COLUMN dispatch_lease_until,DROP COLUMN dispatch_due_at,
    DROP COLUMN dispatch_attempts,DROP COLUMN dispatch_error,DROP COLUMN saga_id,DROP COLUMN dispatch_version;
