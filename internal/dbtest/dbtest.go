// Package dbtest provides shared SQLite setup helpers for tests
// that need a fresh, migrated in-memory database. All resources
// scope to the passed-in testing.TB and clean up automatically.
package dbtest

import (
	"database/sql"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite" // SQLite driver for the in-memory test DB.
)

// OpenMemDB returns an in-memory SQLite handle with the project's
// migrations applied, registered for cleanup on test end.
func OpenMemDB(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		tb.Fatalf("migrate.Apply: %v", err)
	}

	return db
}
