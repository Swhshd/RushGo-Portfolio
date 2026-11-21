-- Business notifications share the transaction of the authoritative change.
-- This adds no financial decision, ticket mutation, or automatic recovery.
CREATE INDEX bookings_operations_window ON bookings(created_at DESC,id DESC);
CREATE INDEX outbox_booking_pending ON outbox(event_key,created_at) WHERE NOT published;
CREATE OR REPLACE FUNCTION enqueue_booking_notification(kind TEXT, source_id UUID, booking UUID, state TEXT) RETURNS void LANGUAGE plpgsql AS $$
DECLARE b bookings%ROWTYPE; identity UUID; body JSONB;
BEGIN
 SELECT * INTO STRICT b FROM bookings WHERE id=booking;
 IF NOT b.managed THEN RETURN; END IF;
 identity := uuid_generate_v5(uuid_ns_url(),'rushgo:notification:'||kind||':'||source_id::text);
 body := jsonb_build_object('version',1,'notification_id',identity::text,'type',kind,'booking_id',b.id::text,'user_id',b.user_id::text,'event_id',b.event_id::text,'generation',b.lifecycle_generation,'currency',upper(b.currency),'total',b.total_amount_cents,'status',state);
 INSERT INTO outbox(id,event_type,event_key,payload,published,created_at) VALUES(identity,'BusinessNotificationV1',b.id::text,body,FALSE,clock_timestamp()) ON CONFLICT(id) DO NOTHING;
 -- An identity collision with different semantics must never be suppressed.
 IF NOT EXISTS(SELECT 1 FROM outbox WHERE id=identity AND event_type='BusinessNotificationV1' AND payload=body) THEN RAISE EXCEPTION 'notification semantic conflict'; END IF;
END $$;
CREATE OR REPLACE FUNCTION notify_ticket_issuance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.outcome='issued' THEN PERFORM enqueue_booking_notification('tickets_issued',NEW.event_id,NEW.booking_id,NEW.outcome);
 ELSE PERFORM enqueue_booking_notification('manual_attention',NEW.event_id,NEW.booking_id,'issuance_denied'); END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER ticket_issuance_notification AFTER INSERT ON ticket_issuance_receipts FOR EACH ROW EXECUTE FUNCTION notify_ticket_issuance();
CREATE OR REPLACE FUNCTION notify_booking_refund() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE kind TEXT;
BEGIN
 IF TG_OP='UPDATE' AND OLD.status=NEW.status THEN RETURN NEW; END IF;
 kind := CASE NEW.status WHEN 'REQUESTED' THEN 'refund_requested' WHEN 'APPROVED' THEN 'refund_approved' WHEN 'REJECTED' THEN 'refund_rejected' WHEN 'REFUNDED' THEN 'refund_succeeded' WHEN 'NEEDS_ATTENTION' THEN 'manual_attention' ELSE NULL END;
 IF kind IS NOT NULL THEN PERFORM enqueue_booking_notification(kind,NEW.request_id,NEW.booking_id,NEW.status); END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER booking_refund_notification AFTER INSERT OR UPDATE OF status ON booking_refund_requests FOR EACH ROW EXECUTE FUNCTION notify_booking_refund();
