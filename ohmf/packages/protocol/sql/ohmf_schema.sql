-- OHMF canonical DB schema (reference) — Section 25

-- 25.1 Users
CREATE TABLE IF NOT EXISTS users (
  user_id UUID PRIMARY KEY,
  primary_phone_e164 TEXT UNIQUE,
  phone_verified_at TIMESTAMP,
  display_name TEXT,
  avatar_url TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- 25.2 Devices
CREATE TABLE IF NOT EXISTS devices (
  device_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  platform TEXT NOT NULL,
  device_name TEXT,
  capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
  public_key TEXT,
  push_token TEXT,
  sms_role_state TEXT,
  last_seen TIMESTAMP
);

-- 25.3 Conversations
CREATE TABLE IF NOT EXISTS conversations (
  conversation_id UUID PRIMARY KEY,
  type TEXT NOT NULL,
  transport_policy TEXT NOT NULL,
  title TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- 25.4 Conversation participants
CREATE TABLE IF NOT EXISTS conversation_participants (
  conversation_id UUID REFERENCES conversations(conversation_id),
  user_id UUID REFERENCES users(user_id),
  role TEXT,
  joined_at TIMESTAMP,
  left_at TIMESTAMP,
  PRIMARY KEY (conversation_id, user_id)
);

-- 25.5 Messages
CREATE TABLE IF NOT EXISTS messages (
  message_id UUID PRIMARY KEY,
  conversation_id UUID REFERENCES conversations(conversation_id),
  server_order BIGINT NOT NULL,
  sender_user_id UUID,
  sender_device_id UUID,
  transport TEXT NOT NULL,
  content_type TEXT NOT NULL,
  content JSONB,
  client_generated_id TEXT,
  created_at TIMESTAMP NOT NULL,
  edited_at TIMESTAMP,
  deleted_at TIMESTAMP,
  redacted_at TIMESTAMP,
  visibility_state TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_conversation_order
ON messages(conversation_id, server_order);

-- 25.6 Message reactions
CREATE TABLE IF NOT EXISTS message_reactions (
  message_id UUID REFERENCES messages(message_id),
  user_id UUID REFERENCES users(user_id),
  emoji TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  removed_at TIMESTAMP,
  PRIMARY KEY (message_id, user_id, emoji)
);

-- 25.7 Deliveries
CREATE TABLE IF NOT EXISTS message_deliveries (
  delivery_id UUID PRIMARY KEY,
  message_id UUID REFERENCES messages(message_id),
  recipient_user_id UUID,
  recipient_device_id UUID,
  recipient_phone_e164 TEXT,
  transport TEXT NOT NULL,
  state TEXT NOT NULL,
  provider TEXT,
  submitted_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  failure_code TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_deliveries_delivered_recipient
ON message_deliveries(message_id, recipient_user_id)
WHERE recipient_user_id IS NOT NULL
  AND state = 'DELIVERED';

-- 25.8 Attachments
CREATE TABLE IF NOT EXISTS attachments (
  attachment_id UUID PRIMARY KEY,
  message_id UUID REFERENCES messages(message_id),
  object_key TEXT,
  thumbnail_key TEXT,
  mime_type TEXT NOT NULL,
  size_bytes BIGINT,
  created_at TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP,
  redacted_at TIMESTAMP
);

-- 25.9 Blocks
CREATE TABLE IF NOT EXISTS user_blocks (
  blocker_user_id UUID NOT NULL,
  blocked_user_id UUID NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (blocker_user_id, blocked_user_id)
);

-- 25.10 Mini-app sessions
CREATE TABLE IF NOT EXISTS mini_app_sessions (
  app_session_id UUID PRIMARY KEY,
  conversation_id UUID NOT NULL,
  app_id TEXT NOT NULL,
  state JSONB NOT NULL,
  state_version INT NOT NULL,
  created_by UUID,
  created_at TIMESTAMP NOT NULL
);

-- 25.11 Mini-app events
CREATE TABLE IF NOT EXISTS mini_app_events (
  app_session_id UUID NOT NULL,
  event_seq INT NOT NULL,
  actor_user_id UUID,
  event_name TEXT NOT NULL,
  body JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (app_session_id, event_seq)
);
