CREATE TABLE IF NOT EXISTS message_reactions (
  message_id UUID REFERENCES messages(id),
  user_id UUID REFERENCES users(id),
  emoji TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, user_id, emoji)
);
