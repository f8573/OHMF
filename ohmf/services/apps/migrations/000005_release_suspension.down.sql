-- Migration: Release Suspension and Kill Switch (Down)
-- Reverts suspension capability additions

DROP INDEX IF EXISTS idx_miniapp_cache_invalidation_events_recent;
DROP INDEX IF EXISTS idx_miniapp_cache_invalidation_events_app_id;
DROP TABLE IF EXISTS miniapp_cache_invalidation_events;

DROP INDEX IF EXISTS idx_miniapp_release_suspension_log_actor;
DROP INDEX IF EXISTS idx_miniapp_release_suspension_log_release;
DROP TABLE IF EXISTS miniapp_release_suspension_log;

DROP INDEX IF EXISTS idx_miniapp_registry_releases_active;
DROP INDEX IF EXISTS idx_miniapp_registry_releases_suspension_status;

ALTER TABLE miniapp_registry_releases
  DROP COLUMN IF EXISTS suspension_reason,
  DROP COLUMN IF EXISTS suspended_at;
