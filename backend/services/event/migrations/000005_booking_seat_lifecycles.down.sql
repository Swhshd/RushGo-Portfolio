DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM booking_seat_lifecycles) OR EXISTS (SELECT 1 FROM seats WHERE lifecycle_generation IS NOT NULL) THEN
        RAISE EXCEPTION 'managed Event lifecycle facts must be retained';
    END IF;
END $$;
DROP INDEX idx_seats_managed_owner;
ALTER TABLE seats DROP CONSTRAINT seats_managed_owner;
ALTER TABLE seats DROP COLUMN lifecycle_generation;
DROP TABLE booking_seat_lifecycles;
