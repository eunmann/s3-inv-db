package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// pingTimeout caps the initial database round-trip OpenStateDB makes
// to verify the connection. Five seconds is generous for SQLite's
// local-file backend and tight enough that a misconfigured DSN fails
// fast.
const pingTimeout = 5 * time.Second

// statePragmas returns the PRAGMA values applied to every connection
// the pool opens. The modernc.org/sqlite driver picks them up from
// _pragma= URL parameters and re-applies them per new connection.
// That matters for the connection-scoped ones (foreign_keys,
// busy_timeout, synchronous, cache_size, temp_store); journal_mode is
// database-wide and sticky, so re-stating it on each connection is a
// harmless no-op.
//
//	journal_mode = WAL    - concurrent readers while a writer commits
//	synchronous  = NORMAL - WAL-safe durability, much faster than FULL
//	foreign_keys = ON     - enforce ON DELETE CASCADE
//	busy_timeout = 5000   - wait up to 5s for the writer lock
//	cache_size   = -20000 - 20MB page cache (negative = KiB)
//	temp_store   = MEMORY - temp btrees in RAM, not in /tmp
func statePragmas() []string {
	return []string{
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"foreign_keys(ON)",
		"busy_timeout(5000)",
		"cache_size(-20000)",
		"temp_store(MEMORY)",
	}
}

// OpenStateDB opens (or creates) the SQLite file backing every domain's
// store and applies statePragmas to every connection. Exposed so the
// server binary's main can wire the shared *sql.DB into Config. The
// ctx bounds the initial ping; pass context.Background() at startup
// where the binary has nothing to inherit from.
func OpenStateDB(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", buildStateDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", path, err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("ping sqlite at %s: %w", path, err)
	}

	return db, nil
}

// buildStateDSN composes the modernc.org/sqlite DSN: the database path
// followed by a _pragma=NAME(VALUE) parameter for each entry of
// statePragmas. Exposed (lowercase) for tests in this package.
func buildStateDSN(path string) string {
	base := path
	hasQuery := strings.Contains(base, "?")
	if base == "" {
		base = "file::memory:?cache=shared"
		hasQuery = true
	}
	pragmas := statePragmas()
	parts := make([]string, 0, len(pragmas))
	for _, p := range pragmas {
		parts = append(parts, "_pragma="+p)
	}
	sep := "?"
	if hasQuery {
		sep = "&"
	}

	return base + sep + strings.Join(parts, "&")
}
