ALTER TABLE users ALTER COLUMN email_canonical SET NOT NULL;
CREATE UNIQUE INDEX ux_users_email_canonical ON users(email_canonical);
