package migrate

import (
	"database/sql"
	"testing"

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
	if err := Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	v, dirty, err := Version(db)
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
	if err := Apply(db); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	v1, _, _ := Version(db)
	if err := Apply(db); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	v2, _, _ := Version(db)
	if v1 != v2 {
		t.Errorf("version drifted across re-apply: %d -> %d", v1, v2)
	}
}

func TestApply_CreatesTables(t *testing.T) {
	db := openMemDB(t)
	if err := Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, table := range []string{"inventories", "jobs"} {
		var name string
		err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Errorf("table %s missing after Apply: %v", table, err)
		}
	}
}

func TestVersion_FreshDBIsZero(t *testing.T) {
	db := openMemDB(t)
	v, dirty, err := Version(db)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if v != 0 || dirty {
		t.Errorf("Version(fresh) = (%d, %v), want (0, false)", v, dirty)
	}
}

func TestDown_RollsBackOneStep(t *testing.T) {
	db := openMemDB(t)
	if err := Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	before, _, _ := Version(db)
	if err := Down(db, 1); err != nil {
		t.Fatalf("Down: %v", err)
	}
	after, _, _ := Version(db)
	if after >= before {
		t.Errorf("version after Down = %d, want < %d", after, before)
	}
}

func TestApply_RecoversFromDirtyState(t *testing.T) {
	db := openMemDB(t)
	if err := Apply(db); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := db.Exec(`UPDATE schema_migrations SET dirty = 1`); err != nil {
		t.Fatalf("seed dirty: %v", err)
	}
	if err := Apply(db); err != nil {
		t.Fatalf("Apply with dirty flag: %v", err)
	}
	_, dirty, _ := Version(db)
	if dirty {
		t.Error("dirty flag still set after Apply")
	}
}

func TestApply_TolerantOfPreExistingTables(t *testing.T) {
	db := openMemDB(t)
	if _, err := db.Exec(`CREATE TABLE inventories (id TEXT PRIMARY KEY, name TEXT, path TEXT, state TEXT, error TEXT, node_count INTEGER, max_depth INTEGER, has_tier_data INTEGER, loaded_at INTEGER, updated_at INTEGER NOT NULL)`); err != nil {
		t.Fatalf("seed legacy table: %v", err)
	}
	if err := Apply(db); err != nil {
		t.Fatalf("Apply on legacy DB: %v", err)
	}
	v, _, _ := Version(db)
	if v < 2 {
		t.Errorf("version = %d, want >= 2", v)
	}
}
