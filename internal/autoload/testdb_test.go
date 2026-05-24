package autoload_test

import (
	"database/sql"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
)

// openTestDB opens a fresh, migrated in-memory SQLite database for
// autoload tests. Each test gets a clean handle via t.Cleanup.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	return dbtest.OpenMemDB(t)
}
