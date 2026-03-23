CREATE TABLE IF NOT EXISTS message_reaction_events (
  id BIGSERIAL PRIMARY KEY,
  message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  acted_by UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  emoji TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('added', 'removed')),
  sent_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  read_at TIMESTAMPTZ,
  acted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_message_reaction_events_message_id_desc
  ON message_reaction_events(message_id, acted_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_message_reaction_events_conversation_id_desc
  ON message_reaction_events(conversation_id, acted_at DESC, id DESC);
