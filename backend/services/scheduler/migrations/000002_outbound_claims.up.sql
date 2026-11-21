ALTER TABLE outbound_events
 ADD COLUMN config_id uuid,
 ADD COLUMN config_version integer,
 ADD COLUMN claim_token uuid,
 ADD COLUMN claim_fence bigint NOT NULL DEFAULT 0,
 ADD COLUMN lease_until timestamptz,
 ADD COLUMN due_at timestamptz,
 ADD COLUMN attempts integer NOT NULL DEFAULT 0,
 ADD COLUMN last_error text;

CREATE UNIQUE INDEX scheduler_outbound_version ON outbound_events(config_id,config_version) WHERE config_id IS NOT NULL;
CREATE INDEX scheduler_outbound_due ON outbound_events(due_at,id) WHERE config_id IS NOT NULL AND status='pending';
ALTER TABLE outbound_events ADD CONSTRAINT scheduler_outbound_identity CHECK (
 (config_id IS NULL AND config_version IS NULL) OR
 (config_id IS NOT NULL AND config_id<>'00000000-0000-0000-0000-000000000000'::uuid
 AND config_version IS NOT NULL AND config_version>0
 AND topic='scheduler.config.events' AND event_type='SchedulerConfigChanged'
 AND status IS NOT NULL AND payload IS NOT NULL AND jsonb_typeof(payload)='object'
 AND claim_fence>=0 AND attempts>=0
 AND ((claim_token IS NULL AND lease_until IS NULL) OR (claim_token IS NOT NULL AND lease_until IS NOT NULL))
 AND ((status='pending' AND published_at IS NULL AND due_at IS NOT NULL)
 OR (status='published' AND published_at IS NOT NULL AND claim_token IS NULL))));
