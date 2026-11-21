DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM payment_provider_events) THEN
        RAISE EXCEPTION 'retained verified provider receipts require explicit archival; refusing destructive down';
    END IF;
END $$;
DROP TABLE payment_provider_events;
