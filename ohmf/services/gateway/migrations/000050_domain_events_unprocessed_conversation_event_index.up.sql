-- Speed same-conversation pending-event checks during replication batch claiming.
CREATE INDEX IF NOT EXISTS idx_domain_events_unprocessed_conversation_event
  ON domain_events (conversation_id, event_id)
  WHERE processed_at IS NULL;
