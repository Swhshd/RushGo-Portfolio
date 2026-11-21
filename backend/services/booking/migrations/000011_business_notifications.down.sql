DROP TRIGGER IF EXISTS booking_refund_notification ON booking_refund_requests;
DROP TRIGGER IF EXISTS ticket_issuance_notification ON ticket_issuance_receipts;
DROP FUNCTION IF EXISTS notify_booking_refund();
DROP FUNCTION IF EXISTS notify_ticket_issuance();
DROP FUNCTION IF EXISTS enqueue_booking_notification(TEXT,UUID,UUID,TEXT);
DROP INDEX IF EXISTS outbox_booking_pending;
DROP INDEX IF EXISTS bookings_operations_window;
-- Already emitted business records are preserved.
