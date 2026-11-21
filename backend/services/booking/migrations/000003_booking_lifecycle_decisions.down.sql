DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM bookings WHERE managed) THEN
        RAISE EXCEPTION 'refusing to remove managed Booking lifecycle decisions; forward repair required';
    END IF;
END $$;
DROP INDEX bookings_managed_expiry;
ALTER TABLE bookings DROP CONSTRAINT bookings_managed_lifecycle;
ALTER TABLE bookings DROP COLUMN managed, DROP COLUMN lifecycle_generation, DROP COLUMN lifecycle_phase,
    DROP COLUMN decision_kind, DROP COLUMN decision_reason, DROP COLUMN decision_id,
    DROP COLUMN confirmation_decision_id, DROP COLUMN currency, DROP COLUMN booking_mode,
    DROP COLUMN reservation_deadline, DROP COLUMN terminal_dispatch_needed;
