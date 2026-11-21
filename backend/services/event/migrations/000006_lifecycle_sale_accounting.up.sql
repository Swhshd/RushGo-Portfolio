ALTER TABLE booking_seat_lifecycles
    ADD COLUMN debits JSONB NULL,
    ADD COLUMN accounted_at TIMESTAMPTZ NULL,
    ADD COLUMN reversed_at TIMESTAMPTZ NULL;
ALTER TABLE booking_seat_lifecycles ADD CONSTRAINT lifecycle_accounting_evidence CHECK (
    (accounting_state='not_applied' AND debits IS NULL AND accounted_at IS NULL AND reversed_at IS NULL) OR
    (accounting_state='applied' AND debits IS NOT NULL AND jsonb_typeof(debits)='object' AND accounted_at IS NOT NULL AND reversed_at IS NULL) OR
    (accounting_state='reversed' AND debits IS NOT NULL AND jsonb_typeof(debits)='object' AND accounted_at IS NOT NULL AND reversed_at IS NOT NULL AND state='closed')
);
ALTER TABLE ticket_tiers ADD COLUMN lifecycle_accounting_baseline TEXT NULL CHECK (lifecycle_accounting_baseline='verified');
-- Preserve and report historical drift; new writes must already satisfy bounds.
ALTER TABLE ticket_tiers ADD CONSTRAINT ticket_tier_quantity_bounds
    CHECK (total_quantity >= 0 AND available_quantity >= 0 AND available_quantity <= total_quantity) NOT VALID;
