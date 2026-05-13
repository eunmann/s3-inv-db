package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenStateDB_AppliesPragmas opens a real on-disk DB and asks SQLite
// to echo back each pragma value. If any pragma drifts from the design
// (someone removes WAL, or adds a value that the driver rejects) this
// test catches it before production does.
func TestOpenStateDB_AppliesPragmas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := openStateDB(path)
	if err != nil {
		t.Fatalf("openStateDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	want := map[string]string{
		"journal_mode": "wal",
		"synchronous":  "1", // NORMAL
		"foreign_keys": "1",
		"busy_timeout": "5000",
		"cache_size":   "-20000",
		"temp_store":   "2", // MEMORY
	}
	for name, expect := range want {
		var got string
		if err := db.QueryRow("PRAGMA " + name).Scan(&got); err != nil {
			t.Errorf("query PRAGMA %s: %v", name, err)
			continue
		}
		if !strings.EqualFold(got, expect) {
			t.Errorf("PRAGMA %s = %q, want %q", name, got, expect)
		}
	}
}

func TestBuildDSN_AddsPragmasToFilePath(t *testing.T) {
	dsn := buildDSN("/cache/state.db")
	for _, p := range statePragmas {
		if !strings.Contains(dsn, "_pragma="+p) {
			t.Errorf("DSN missing _pragma=%s\nDSN: %s", p, dsn)
		}
	}
	if !strings.HasPrefix(dsn, "/cache/state.db?") {
		t.Errorf("DSN should start with the path + ?, got %q", dsn)
	}
}

func TestBuildDSN_AppendsToExistingQuery(t *testing.T) {
	dsn := buildDSN("")
	if !strings.HasPrefix(dsn, "file::memory:?cache=shared&_pragma=") {
		t.Errorf("in-memory DSN must keep cache=shared and append with &, got %q", dsn)
	}
}
