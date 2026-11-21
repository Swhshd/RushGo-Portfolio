ALTER TABLE scheduler_configs
    ADD COLUMN job_type VARCHAR(50) NOT NULL DEFAULT 'legacy-unsupported';

-- The only preexisting executable handler is keyed by this exact name.
-- Keep every other legacy row quarantined instead of silently routing it to
-- the reservation cleaner after deployment.
UPDATE scheduler_configs
SET job_type = 'reservation-cleaner-job'
WHERE name = 'reservation-cleaner-job';

ALTER TABLE scheduler_configs
    ADD CONSTRAINT scheduler_configs_job_type_check
    CHECK (job_type IN ('reservation-cleaner-job', 'legacy-unsupported'));
