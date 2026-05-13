package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// openStateDB opens (or creates) the SQLite file backing every domain's
// store. Pragmas:
//
//	foreign_keys(1)        — enforces ON DELETE CASCADE for downstream tables
//	journal_mode(WAL)      — concurrent readers + writers (SSE while job runs)
//	busy_timeout(5000)     — wait up to 5s when another connection holds the lock
//
// The path "" opens an anonymous in-memory DB, used by tests.
func openStateDB(path string) (*sql.DB, error) {
	dsn := path
	if dsn == "" {
		dsn = "file::memory:?cache=shared"
	}
	dsn += "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", path, err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite at %s: %w", path, err)
	}
	return db, nil
}
