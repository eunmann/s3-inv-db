// Package dbtest provides shared SQLite setup helpers for tests
// that need a fresh, migrated in-memory database. All resources
// scope to the passed-in *testing.T and clean up automatically.
package dbtest

import (
	"database/sql"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite" // SQLite driver for the in-memory test DB.
)

// OpenMemDB returns an in-memory SQLite handle with the project's
// migrations applied, registered for cleanup on test end.
func OpenMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("migrate.Apply: %v", err)
	}

	return db
}
