WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY message_id, recipient_user_id
      ORDER BY submitted_at NULLS LAST, updated_at NULLS LAST, id
    ) AS rn
  FROM message_deliveries
  WHERE recipient_user_id IS NOT NULL
    AND state = 'DELIVERED'
)
DELETE FROM message_deliveries md
USING ranked
WHERE md.ctid = ranked.ctid
  AND ranked.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_deliveries_delivered_recipient
ON message_deliveries(message_id, recipient_user_id)
WHERE recipient_user_id IS NOT NULL
  AND state = 'DELIVERED';
