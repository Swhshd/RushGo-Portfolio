DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM simulated_provider_state) THEN
   RAISE EXCEPTION 'refusing to discard simulated provider effects';
 END IF;
END $$;
DROP TABLE simulated_provider_state;
