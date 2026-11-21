ALTER TABLE bookings
    ADD COLUMN request_id TEXT,
    ADD COLUMN request_hash BYTEA,
    ADD COLUMN dispatch_kind TEXT,
    ADD COLUMN dispatch_state TEXT,
    ADD COLUMN dispatch_token UUID,
    ADD COLUMN dispatch_fence BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_lease_until TIMESTAMPTZ,
    ADD COLUMN dispatch_due_at TIMESTAMPTZ,
    ADD COLUMN dispatch_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_error TEXT,
    ADD COLUMN saga_id UUID,
    ADD COLUMN dispatch_version INTEGER;
ALTER TABLE bookings ADD CONSTRAINT bookings_request_identity CHECK (
    (request_id IS NULL AND request_hash IS NULL) OR
    (managed AND request_id IS NOT NULL AND request_id ~ '^[A-Za-z0-9_-]{1,128}$' AND request_hash IS NOT NULL AND octet_length(request_hash)=32));
ALTER TABLE bookings ADD CONSTRAINT bookings_dispatch_shape CHECK (
    ((dispatch_token IS NULL AND dispatch_lease_until IS NULL) OR (dispatch_token IS NOT NULL AND dispatch_lease_until IS NOT NULL))
    AND dispatch_fence>=0 AND dispatch_attempts>=0
    AND (dispatch_kind IS NULL OR dispatch_kind IN ('start','settle'))
    AND (dispatch_state IS NULL OR (dispatch_kind IS NOT NULL AND dispatch_state IN ('pending','claimed','accepted','attention') AND dispatch_version IS NOT NULL AND dispatch_due_at IS NOT NULL)));
CREATE UNIQUE INDEX bookings_user_request_identity ON bookings(user_id,request_id) WHERE request_id IS NOT NULL;
CREATE INDEX bookings_dispatch_due ON bookings(dispatch_due_at,id) WHERE managed AND dispatch_state IN ('pending','claimed');
UPDATE bookings SET dispatch_kind='settle',dispatch_state='pending',dispatch_due_at=clock_timestamp(),dispatch_version=version
WHERE managed AND terminal_dispatch_needed AND lifecycle_phase='TERMINAL' AND status<>'CONFIRMED';
