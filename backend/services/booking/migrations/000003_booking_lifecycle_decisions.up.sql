ALTER TABLE bookings
    ADD COLUMN managed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN lifecycle_generation BIGINT,
    ADD COLUMN lifecycle_phase TEXT,
    ADD COLUMN decision_kind TEXT,
    ADD COLUMN decision_reason TEXT,
    ADD COLUMN decision_id UUID,
    ADD COLUMN confirmation_decision_id UUID,
    ADD COLUMN currency TEXT,
    ADD COLUMN booking_mode TEXT,
    ADD COLUMN reservation_deadline TIMESTAMPTZ,
    ADD COLUMN terminal_dispatch_needed BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE bookings ADD CONSTRAINT bookings_managed_lifecycle CHECK (NOT managed OR (
    lifecycle_generation IS NOT NULL AND lifecycle_generation=1
    AND lifecycle_phase IS NOT NULL AND lifecycle_phase IN ('OPEN','CONFIRMING','TERMINAL')
    AND currency IS NOT NULL AND currency='usd'
    AND booking_mode IS NOT NULL AND booking_mode IN ('pessimistic','optimistic')
    AND reservation_deadline IS NOT NULL AND total_amount_cents BETWEEN 1 AND 2147483647
    AND version>=1
    AND ((lifecycle_phase IN ('OPEN','CONFIRMING') AND status='PENDING')
         OR (lifecycle_phase='TERMINAL' AND status IN ('CONFIRMED','FAILED','CANCELLED','EXPIRED')))
    AND (lifecycle_phase<>'CONFIRMING' OR confirmation_decision_id IS NOT NULL)
    AND (lifecycle_phase<>'TERMINAL' OR (decision_id IS NOT NULL AND decision_kind IS NOT NULL))
));
CREATE INDEX bookings_managed_expiry ON bookings (reservation_deadline,id) WHERE managed AND lifecycle_phase='OPEN';
