-- Credential epochs cannot be safely rolled back: doing so could revive old JWTs.
DO $$ BEGIN RAISE EXCEPTION 'account security migration requires a reviewed forward recovery'; END $$;
