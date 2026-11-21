DROP TABLE IF EXISTS password_reset_requests;
DROP TABLE IF EXISTS password_reset_guards;
DROP TABLE IF EXISTS email_verification_requests;
DROP TABLE IF EXISTS email_verification_guards;
ALTER TABLE users DROP COLUMN IF EXISTS email_verified_at;
ALTER TABLE users DROP COLUMN IF EXISTS email_canonical;
