DROP TABLE IF EXISTS event_reminder_outbox;
DROP TABLE IF EXISTS event_reminder_fanouts;
DROP TABLE IF EXISTS event_interests;
ALTER TABLE events DROP COLUMN IF EXISTS first_sale_opened_at, DROP COLUMN IF EXISTS sale_notification_generation, DROP COLUMN IF EXISTS sales_generation;
