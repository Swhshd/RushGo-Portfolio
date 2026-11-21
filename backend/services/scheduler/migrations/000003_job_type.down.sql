ALTER TABLE scheduler_configs
    DROP CONSTRAINT IF EXISTS scheduler_configs_job_type_check;

ALTER TABLE scheduler_configs
    DROP COLUMN IF EXISTS job_type;
