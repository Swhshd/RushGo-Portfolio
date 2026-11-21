DROP INDEX IF EXISTS idx_events_publication_date;
DROP TABLE IF EXISTS event_creation_requests;
ALTER TABLE ticket_tiers
    DROP CONSTRAINT IF EXISTS ticket_tier_total_quantity_bounds,
    DROP CONSTRAINT IF EXISTS ticket_tier_price_bounds;
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS events_sales_window_check,
    DROP CONSTRAINT IF EXISTS events_sales_status_check,
    DROP CONSTRAINT IF EXISTS events_publication_status_check,
    DROP COLUMN IF EXISTS sales_end_at,
    DROP COLUMN IF EXISTS sales_start_at,
    DROP COLUMN IF EXISTS sales_status,
    DROP COLUMN IF EXISTS publication_status;
