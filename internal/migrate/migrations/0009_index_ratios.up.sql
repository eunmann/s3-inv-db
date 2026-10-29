CREATE TABLE IF NOT EXISTS index_ratios (
    source      TEXT NOT NULL,
    name        TEXT NOT NULL,
    file_format TEXT NOT NULL,
    ratio       REAL NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (source, name)
);
