CREATE INDEX ticket_admission_stale_observation ON ticket_admission_intents(created_at) WHERE state='PENDING';
CREATE INDEX booking_outbox_stale_observation ON outbox(created_at) WHERE NOT published;
