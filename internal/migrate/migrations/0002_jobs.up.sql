CREATE TABLE jobs (
    id            TEXT PRIMARY KEY,
    inventory_id  TEXT NOT NULL REFERENCES inventories(id) ON DELETE CASCADE,
    kind          TEXT NOT NULL,
    state         TEXT NOT NULL,
    stage         TEXT NOT NULL DEFAULT '',
    progress      INTEGER NOT NULL DEFAULT 0,
    bytes_total   INTEGER NOT NULL DEFAULT 0,
    bytes_done    INTEGER NOT NULL DEFAULT 0,
    started_at    INTEGER NOT NULL DEFAULT 0,
    finished_at   INTEGER NOT NULL DEFAULT 0,
    error         TEXT NOT NULL DEFAULT '',
    updated_at    INTEGER NOT NULL
);

CREATE INDEX idx_jobs_inventory ON jobs(inventory_id);
CREATE INDEX idx_jobs_state     ON jobs(state);
