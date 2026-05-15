DROP TABLE IF EXISTS inventory_configs;

ALTER TABLE inventories DROP COLUMN last_accessed_at;
ALTER TABLE inventories DROP COLUMN auto_load_backoff_until;
ALTER TABLE inventories DROP COLUMN auto_load_failure_count;
ALTER TABLE inventories DROP COLUMN index_bytes;
ALTER TABLE inventories DROP COLUMN user_unloaded_at;
ALTER TABLE inventories DROP COLUMN pinned;
