-- Migration: Release Suspension and Kill Switch
-- Date: 2026-03-21
--
-- Adds release suspension capability for admin kill switches and fast cache invalidation.
-- Enables blocking new sessions from launching suspended/revoked releases and gracefully
-- terminating active sessions.

-- Add suspension tracking to releases
ALTER TABLE miniapp_registry_releases
  ADD COLUMN IF NOT EXISTS suspended_at timestamptz,
  ADD COLUMN IF NOT EXISTS suspension_reason text DEFAULT '';

-- Create index for fast lookup of suspended/revoked releases
CREATE INDEX IF NOT EXISTS idx_miniapp_registry_releases_suspension_status
  ON miniapp_registry_releases (app_id)
  WHERE suspended_at IS NOT NULL OR revoked_at IS NOT NULL;

-- Create index for temporal queries (active releases)
CREATE INDEX IF NOT EXISTS idx_miniapp_registry_releases_active
  ON miniapp_registry_releases (app_id, version)
  WHERE suspended_at IS NULL AND revoked_at IS NULL;

-- Create table for suspension audit trail
CREATE TABLE IF NOT EXISTS miniapp_release_suspension_log (
  id bigserial PRIMARY KEY,
  app_id text NOT NULL,
  version text NOT NULL,
  action text NOT NULL,
  actor_user_id text NOT NULL,
  reason text NOT NULL DEFAULT '',
  metadata_json jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (app_id, version) REFERENCES miniapp_registry_releases(app_id, version) ON DELETE CASCADE
);

-- Create index for fast suspension log queries
CREATE INDEX IF NOT EXISTS idx_miniapp_release_suspension_log_release
  ON miniapp_release_suspension_log (app_id, version, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_miniapp_release_suspension_log_actor
  ON miniapp_release_suspension_log (actor_user_id, created_at DESC);

-- Create table for tracking cache invalidation events
CREATE TABLE IF NOT EXISTS miniapp_cache_invalidation_events (
  id bigserial PRIMARY KEY,
  event_type text NOT NULL,
  app_id text NOT NULL,
  version text,
  affected_sessions bigint,
  invalidated_at timestamptz NOT NULL DEFAULT now(),
  propagation_latency_ms integer,
  metadata_json jsonb NOT NULL DEFAULT '{}'
);

-- Create index for recent invalidation events
CREATE INDEX IF NOT EXISTS idx_miniapp_cache_invalidation_events_app_id
  ON miniapp_cache_invalidation_events (app_id, invalidated_at DESC);

CREATE INDEX IF NOT EXISTS idx_miniapp_cache_invalidation_events_recent
  ON miniapp_cache_invalidation_events (invalidated_at DESC);
