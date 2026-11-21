DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM booking_seat_lifecycles WHERE accounting_state <> 'not_applied') OR
       EXISTS (SELECT 1 FROM ticket_tiers WHERE lifecycle_accounting_baseline IS NOT NULL) THEN
        RAISE EXCEPTION 'managed sale accounting evidence must be retained';
    END IF;
END $$;
ALTER TABLE ticket_tiers DROP CONSTRAINT ticket_tier_quantity_bounds;
ALTER TABLE ticket_tiers DROP COLUMN lifecycle_accounting_baseline;
ALTER TABLE booking_seat_lifecycles DROP CONSTRAINT lifecycle_accounting_evidence;
ALTER TABLE booking_seat_lifecycles DROP COLUMN debits, DROP COLUMN accounted_at, DROP COLUMN reversed_at;
