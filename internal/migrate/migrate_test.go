package migrate_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite"
)

func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func TestApply_FreshDBReachesLatest(t *testing.T) {
	db := openMemDB(t)
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	info, err := migrate.Version(db)
	v, dirty := info.Version, info.Dirty
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if dirty {
		t.Errorf("dirty = true after Apply")
	}
	if v < 2 {
		t.Errorf("version = %d, want >= 2 (inventories + jobs)", v)
	}
}

func TestApply_Idempotent(t *testing.T) {
	db := openMemDB(t)
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	info1, _ := migrate.Version(db)
	v1 := info1.Version
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	info2, _ := migrate.Version(db)
	v2 := info2.Version
	if v1 != v2 {
		t.Errorf("version drifted across re-apply: %d -> %d", v1, v2)
	}
}

func TestApply_CreatesTables(t *testing.T) {
	db := openMemDB(t)
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	ctx := context.Background()
	for _, table := range []string{"inventories", "jobs"} {
		var name string
		err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Errorf("table %s missing after Apply: %v", table, err)
		}
	}
}

func TestVersion_FreshDBIsZero(t *testing.T) {
	db := openMemDB(t)
	info, err := migrate.Version(db)
	v, dirty := info.Version, info.Dirty
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if v != 0 || dirty {
		t.Errorf("Version(fresh) = (%d, %v), want (0, false)", v, dirty)
	}
}

func TestApply_RecoversFromDirtyState(t *testing.T) {
	db := openMemDB(t)
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET dirty = 1`); err != nil {
		t.Fatalf("seed dirty: %v", err)
	}
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("Apply with dirty flag: %v", err)
	}
	info, _ := migrate.Version(db)
	if info.Dirty {
		t.Error("dirty flag still set after Apply")
	}
}

func TestApply_TolerantOfPreExistingTables(t *testing.T) {
	db := openMemDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE inventories (id TEXT PRIMARY KEY, name TEXT, path TEXT, state TEXT, error TEXT, node_count INTEGER, max_depth INTEGER, has_tier_data INTEGER, loaded_at INTEGER, updated_at INTEGER NOT NULL)`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("Apply on legacy DB: %v", err)
	}
	info2, _ := migrate.Version(db)
	if info2.Version < 2 {
		t.Errorf("version = %d, want >= 2", info2.Version)
	}
}
