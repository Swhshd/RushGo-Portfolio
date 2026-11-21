-- Local development bootstrap only. Do not add this file as a migration.
-- Fresh databases have no reservation-cleaner-job row; an insert with the
-- schema default timeout=0 is rejected by the scheduler as invalid.
INSERT INTO scheduler_configs (id, name, job_type, timeout, version, interval_expression, is_enable)
VALUES ('cccccccc-cccc-4ccc-8ccc-cccccccccccc',
        'reservation-cleaner-job', 'reservation-cleaner-job', 30, 1, '*/30 * * * * *', TRUE)
ON CONFLICT (name) DO UPDATE
SET job_type = EXCLUDED.job_type,
    timeout = EXCLUDED.timeout,
    updated_at = clock_timestamp()
WHERE scheduler_configs.job_type = 'legacy-unsupported'
   OR scheduler_configs.timeout IS NULL
   OR scheduler_configs.timeout <= 0;
