ALTER TABLE message_edits
  DROP COLUMN IF EXISTS read_at,
  DROP COLUMN IF EXISTS delivered_at,
  DROP COLUMN IF EXISTS sent_at;
