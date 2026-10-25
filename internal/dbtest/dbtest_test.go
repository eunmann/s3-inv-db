package dbtest_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
)

// TestOpenMemDBIsolated: a row written through one handle is invisible
// to another handle opened while the first is still open.
func TestOpenMemDBIsolated(t *testing.T) {
	a := dbtest.OpenMemDB(t)
	if _, err := a.ExecContext(t.Context(), "CREATE TABLE probe (v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	b := dbtest.OpenMemDB(t)
	var n int
	if err := b.QueryRowContext(t.Context(), "SELECT count(*) FROM sqlite_master WHERE name = 'probe'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("second OpenMemDB sees the first one's table: the databases are shared")
	}
	// Within one handle, every pooled connection sees the same database.
	a.SetMaxOpenConns(4)
	conns := make([]interface{ Close() error }, 0, 4)
	for range 4 {
		c, err := a.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.QueryRowContext(t.Context(), "SELECT count(*) FROM probe").Scan(&n); err != nil {
			t.Fatalf("pooled connection does not see probe: %v", err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		_ = c.Close()
	}
}
