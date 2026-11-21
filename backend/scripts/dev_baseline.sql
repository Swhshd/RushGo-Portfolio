-- Development/test-only baseline initialization. Never add as a migration.
-- The caller must set both GUCs in the same psql session:
--   SET ticketbox.dev_baseline = 'on';
--   SET ticketbox.event_ids = '<uuid>[,<uuid>...]';
-- Only freshly initialized, fully covered, entirely available tiers can be
-- marked. Existing bookings, seats, availability, lifecycles, and ledgers are
-- read-only here.
BEGIN;

DO $$
BEGIN
  IF coalesce(current_setting('ticketbox.dev_baseline', true), '') <> 'on' THEN
    RAISE EXCEPTION 'refusing: SET ticketbox.dev_baseline=on explicitly';
  END IF;
  IF coalesce(current_setting('ticketbox.event_ids', true), '') = '' THEN
    RAISE EXCEPTION 'refusing: SET ticketbox.event_ids to the freshly created events';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM ticket_tiers
    WHERE event_id = ANY(string_to_array(current_setting('ticketbox.event_ids'), ',')::uuid[])
  ) THEN
    RAISE EXCEPTION 'refusing: no tiers found for targeted events';
  END IF;
END $$;

-- Serialize the invariant check and baseline write against seat/ledger writes.
SET LOCAL lock_timeout = '5s';
LOCK TABLE ticket_tiers, seats, booking_seat_lifecycles IN SHARE ROW EXCLUSIVE MODE;

UPDATE ticket_tiers t
SET lifecycle_accounting_baseline = 'verified'
WHERE t.event_id = ANY(string_to_array(current_setting('ticketbox.event_ids'), ',')::uuid[])
  AND t.lifecycle_accounting_baseline IS NULL
  AND t.total_quantity > 0
  AND t.available_quantity = t.total_quantity
  AND (SELECT count(*) FROM seats s
       WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NULL) = t.total_quantity
  AND NOT EXISTS (SELECT 1 FROM seats s
                  WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM seats s
                  WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NULL
                    AND (s.status <> 'available'
                      OR s.booking_id IS NOT NULL
                      OR s.reserved_by_booking_id IS NOT NULL
                      OR s.reservation_expired_at IS NOT NULL
                      OR s.lifecycle_generation IS NOT NULL))
  AND NOT EXISTS (SELECT 1 FROM booking_seat_lifecycles l
                  WHERE l.accounting_state = 'applied'
                    AND jsonb_exists(l.debits, t.id::text));

DO $$
DECLARE missing integer;
BEGIN
  SELECT count(*) INTO missing
  FROM ticket_tiers t
  WHERE t.event_id = ANY(string_to_array(current_setting('ticketbox.event_ids'), ',')::uuid[])
    AND t.total_quantity > 0
    AND (
      t.lifecycle_accounting_baseline IS DISTINCT FROM 'verified'
      OR t.available_quantity <> t.total_quantity
      OR (SELECT count(*) FROM seats s
          WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NULL) <> t.total_quantity
      OR EXISTS (SELECT 1 FROM seats s
                 WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NOT NULL)
      OR EXISTS (SELECT 1 FROM seats s
                 WHERE s.ticket_tier_id = t.id AND s.deleted_at IS NULL
                   AND (s.status <> 'available'
                     OR s.booking_id IS NOT NULL
                     OR s.reserved_by_booking_id IS NOT NULL
                     OR s.reservation_expired_at IS NOT NULL
                     OR s.lifecycle_generation IS NOT NULL))
      OR EXISTS (SELECT 1 FROM booking_seat_lifecycles l
                 WHERE l.accounting_state = 'applied'
                   AND jsonb_exists(l.debits, t.id::text))
    );
  IF missing > 0 THEN
    RAISE EXCEPTION '% targeted tier(s) failed the baseline invariant', missing;
  END IF;
END $$;

COMMIT;
