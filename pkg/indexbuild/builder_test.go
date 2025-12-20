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

	// Create and populate SQLite database using MemoryAggregator
	testObjects := []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"a/file1.txt", 100, tiers.Standard},
		{"a/file2.txt", 200, tiers.Standard},
		{"b/file3.txt", 300, tiers.GlacierFR},
	}

	memAgg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	for _, obj := range testObjects {
		memAgg.AddObject(obj.key, obj.size, obj.tierID)
	}
	if err := memAgg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	sqliteCfg := sqliteagg.DefaultConfig(dbPath)

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

func TestBuildFromSQLiteEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Create empty SQLite database using MemoryAggregator
	memAgg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	if err := memAgg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	sqliteCfg := sqliteagg.DefaultConfig(dbPath)

	cfg := SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteCfg,
	}

	// Build should fail with empty database
	err := BuildFromSQLite(cfg)
	if err == nil {
		t.Fatal("expected error for empty database")
	}
	if err.Error() != "no nodes built from SQLite database" {
		t.Errorf("unexpected error: %v", err)
	}
}
