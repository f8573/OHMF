CREATE TABLE IF NOT EXISTS user_privacy_preferences (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  send_read_receipts BOOLEAN NOT NULL DEFAULT true,
  share_presence BOOLEAN NOT NULL DEFAULT true,
  share_typing BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_privacy_preferences_updated_at
  ON user_privacy_preferences (updated_at DESC);
