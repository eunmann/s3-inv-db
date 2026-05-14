CREATE TABLE inventories (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    path          TEXT NOT NULL,
    state         TEXT NOT NULL,
    error         TEXT NOT NULL DEFAULT '',
    node_count    INTEGER NOT NULL DEFAULT 0,
    max_depth     INTEGER NOT NULL DEFAULT 0,
    has_tier_data INTEGER NOT NULL DEFAULT 0,
    loaded_at     INTEGER NOT NULL DEFAULT 0,
    updated_at    INTEGER NOT NULL
);
