ALTER TABLE inventories ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inventories ADD COLUMN user_unloaded_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inventories ADD COLUMN index_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inventories ADD COLUMN auto_load_failure_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inventories ADD COLUMN auto_load_backoff_until INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inventories ADD COLUMN last_accessed_at INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS inventory_configs (
    source              TEXT NOT NULL,
    name                TEXT NOT NULL,
    auto_load           INTEGER NOT NULL DEFAULT 0,
    retention_count     INTEGER NOT NULL DEFAULT 2,
    poll_failure_count  INTEGER NOT NULL DEFAULT 0,
    poll_backoff_until  INTEGER NOT NULL DEFAULT 0,
    last_polled_at      INTEGER NOT NULL DEFAULT 0,
    last_poll_error     TEXT NOT NULL DEFAULT '',
    updated_at          INTEGER NOT NULL,
    PRIMARY KEY (source, name)
);
