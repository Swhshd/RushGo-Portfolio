-- Removing the presentation feature leaves the audit available for inspection.
-- A later re-up must use a forward migration; do not erase prior changes.
DO $$ BEGIN RAISE EXCEPTION 'management audit is retained; use a forward corrective migration'; END $$;
