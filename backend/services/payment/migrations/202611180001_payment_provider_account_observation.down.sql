DROP TRIGGER IF EXISTS payment_provider_account_observation_immutable
    ON payment_provider_account_observation;
DROP TRIGGER IF EXISTS payment_provider_account_observation_no_truncate
    ON payment_provider_account_observation;
DROP FUNCTION IF EXISTS payment_provider_account_observation_reject_mutation();
DROP TABLE IF EXISTS payment_provider_account_observation;
