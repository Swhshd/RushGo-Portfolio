DROP INDEX IF EXISTS ux_users_email_canonical;
ALTER TABLE users ALTER COLUMN email_canonical DROP NOT NULL;
