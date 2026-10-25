// Package dbtest provides shared SQLite setup helpers for tests
// that need a fresh, migrated in-memory database. All resources
// scope to the passed-in testing.TB and clean up automatically.
package dbtest

import (
	"crypto/rand"
	"database/sql"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite" // SQLite driver for the in-memory test DB.
)

// OpenMemDB returns an in-memory SQLite handle with the project's
// migrations applied, registered for cleanup on test end.
//
// Each call gets its own randomly named database. A single shared
// "file::memory:" lives as long as any connection to it, so a test's
// background job still persisting after its cleanup kept it alive and
// wrote rows the next test then read (#75). The pool's connections
// still share one database through cache=shared.
func OpenMemDB(tb testing.TB) *sql.DB {
	tb.Helper()
	dsn := "file:memdb-" + rand.Text() + "?mode=memory&cache=shared&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		tb.Fatalf("sql.Open: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		tb.Fatalf("migrate.Apply: %v", err)
	}

	return db
}
