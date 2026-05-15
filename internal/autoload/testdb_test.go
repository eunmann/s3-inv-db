package autoload_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// openTestDB opens an in-memory SQLite database for autoload tests.
// Foreign keys on for parity with production wiring; each test gets a
// fresh DB via t.Cleanup.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}
