-- Drop the partial index used for unprocessed per-conversation event claims.
DROP INDEX IF EXISTS idx_domain_events_unprocessed_conversation_event;
