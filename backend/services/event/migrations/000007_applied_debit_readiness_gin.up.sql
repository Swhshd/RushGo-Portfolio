CREATE INDEX booking_seat_lifecycles_applied_debits_gin
    ON booking_seat_lifecycles USING GIN (debits jsonb_ops)
    WHERE accounting_state = 'applied';
