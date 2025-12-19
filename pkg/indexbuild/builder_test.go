package indexbuild

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestBuildFromSQLiteValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     SQLiteConfig
		wantErr string
	}{
		{
			name:    "missing output dir",
			cfg:     SQLiteConfig{DBPath: "/tmp/test.db"},
			wantErr: "output directory required",
		},
		{
			name:    "missing db path",
			cfg:     SQLiteConfig{OutDir: "/tmp/out"},
			wantErr: "database path required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := BuildFromSQLite(tt.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestBuildFromSQLiteSimple(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Create and populate SQLite database
	sqliteCfg := sqliteagg.DefaultConfig(dbPath)
	agg, err := sqliteagg.Open(sqliteCfg)
	if err != nil {
		t.Fatalf("Open SQLite failed: %v", err)
	}

	// Add test objects
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	testObjects := []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"a/file1.txt", 100, tiers.Standard},
		{"a/file2.txt", 200, tiers.Standard},
		{"b/file3.txt", 300, tiers.GlacierFR},
	}

	for _, obj := range testObjects {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s) failed: %v", obj.key, err)
		}
	}

	if err := agg.MarkChunkDone("chunk1"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Build index from SQLite
	cfg := SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteCfg,
	}

	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	// Verify output directory was created
	if _, err := os.Stat(outDir); err != nil {
		t.Errorf("output directory not created: %v", err)
	}

	// Verify key files exist
	expectedFiles := []string{
		"manifest.json",
		"subtree_end.u64",
		"object_count.u64",
		"total_bytes.u64",
		"depth.u32",
		"max_depth_in_subtree.u32",
		"mph.bin",
	}
	for _, name := range expectedFiles {
		path := filepath.Join(outDir, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file %q not found: %v", name, err)
		}
	}

	// Verify tier_stats was created (since we used different tiers)
	tierStatsDir := filepath.Join(outDir, "tier_stats")
	if _, err := os.Stat(tierStatsDir); err != nil {
		t.Errorf("tier_stats directory not created: %v", err)
	}
}

func TestBuildFromSQLiteResume(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Create and populate SQLite database
	sqliteCfg := sqliteagg.DefaultConfig(dbPath)
	agg, err := sqliteagg.Open(sqliteCfg)
	if err != nil {
		t.Fatalf("Open SQLite failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	cfg := SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteCfg,
	}

	// First build
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("First BuildFromSQLite failed: %v", err)
	}

	// Get modification time of manifest
	manifestPath := filepath.Join(outDir, "manifest.json")
	info1, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	modTime1 := info1.ModTime()

	// Second build should skip (resume from completed build)
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("Second BuildFromSQLite failed: %v", err)
	}

	// Manifest should NOT be modified (build was skipped)
	info2, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after second build: %v", err)
	}
	modTime2 := info2.ModTime()

	if !modTime1.Equal(modTime2) {
		t.Errorf("manifest was modified during resume - expected skip")
	}
}

func TestBuildFromSQLiteEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Create empty SQLite database
	sqliteCfg := sqliteagg.DefaultConfig(dbPath)
	agg, err := sqliteagg.Open(sqliteCfg)
	if err != nil {
		t.Fatalf("Open SQLite failed: %v", err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	cfg := SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteCfg,
	}

	// Build should fail with empty database
	err = BuildFromSQLite(cfg)
	if err == nil {
		t.Fatal("expected error for empty database")
	}
	if err.Error() != "no nodes built from SQLite database" {
		t.Errorf("unexpected error: %v", err)
	}
}
