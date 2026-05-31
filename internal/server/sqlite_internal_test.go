package server

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
	db, err := OpenStateDB(t.Context(), path)
	if err != nil {
		t.Fatalf("OpenStateDB: %v", err)
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
	ctx := t.Context()
	for name, expect := range want {
		var got string
		if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil {
			t.Errorf("query PRAGMA %s: %v", name, err)

			continue
		}
		if !strings.EqualFold(got, expect) {
			t.Errorf("PRAGMA %s = %q, want %q", name, got, expect)
		}
	}
}

func TestBuildStateDSN_AddsPragmasToFilePath(t *testing.T) {
	dsn := buildStateDSN("/cache/state.db")
	for _, p := range statePragmas() {
		if !strings.Contains(dsn, "_pragma="+p) {
			t.Errorf("DSN missing _pragma=%s\nDSN: %s", p, dsn)
		}
	}
	if !strings.HasPrefix(dsn, "/cache/state.db?") {
		t.Errorf("DSN should start with the path + ?, got %q", dsn)
	}
}

func TestBuildStateDSN_AppendsToExistingQuery(t *testing.T) {
	dsn := buildStateDSN("")
	if !strings.HasPrefix(dsn, "file::memory:?cache=shared&_pragma=") {
		t.Errorf("in-memory DSN must keep cache=shared and append with &, got %q", dsn)
	}
}

func TestResolveStateDBPath(t *testing.T) {
	cases := []struct {
		name     string
		cacheDir string
		want     string
	}{
		{"cache-dir provides location", "/var/cache/s3inv", "/var/cache/s3inv/state.db"},
		{"empty cache-dir stays empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveStateDBPath(tc.cacheDir); got != tc.want {
				t.Errorf("resolveStateDBPath(%q) = %q, want %q",
					tc.cacheDir, got, tc.want)
			}
		})
	}
}
