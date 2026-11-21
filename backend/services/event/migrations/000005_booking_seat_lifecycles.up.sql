CREATE TABLE booking_seat_lifecycles (
    booking_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation = 1),
    event_id UUID NOT NULL REFERENCES events(id),
    seat_ids UUID[] NOT NULL CHECK (cardinality(seat_ids) > 0),
    seat_set_hash BYTEA NOT NULL CHECK (octet_length(seat_set_hash) = 32),
    reservation_deadline TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('held', 'booked', 'closed')),
    accounting_state TEXT NOT NULL DEFAULT 'not_applied' CHECK (accounting_state IN ('not_applied', 'applied', 'reversed')),
    last_operation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (booking_id, generation)
);

ALTER TABLE seats ADD COLUMN lifecycle_generation BIGINT NULL CHECK (lifecycle_generation = 1);
ALTER TABLE seats ADD CONSTRAINT seats_managed_owner CHECK (
    lifecycle_generation IS NULL OR
    (status = 'reserved' AND reserved_by_booking_id IS NOT NULL AND reservation_expired_at IS NOT NULL AND booking_id IS NULL) OR
    (status = 'booked' AND booking_id IS NOT NULL AND reserved_by_booking_id IS NULL AND reservation_expired_at IS NULL)
);
CREATE INDEX idx_seats_managed_owner ON seats (COALESCE(booking_id, reserved_by_booking_id), lifecycle_generation)
WHERE lifecycle_generation IS NOT NULL AND deleted_at IS NULL;
