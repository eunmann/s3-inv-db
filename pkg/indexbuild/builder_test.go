package indexbuild

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ChunkSize != 1_000_000 {
		t.Errorf("ChunkSize = %d, want 1000000", cfg.ChunkSize)
	}
	if cfg.OutDir != "" {
		t.Errorf("OutDir = %q, want empty", cfg.OutDir)
	}
	if cfg.TmpDir != "" {
		t.Errorf("TmpDir = %q, want empty", cfg.TmpDir)
	}
	if cfg.TrackTiers {
		t.Error("TrackTiers = true, want false")
	}
}

func TestBuildValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		cfg     Config
		files   []string
		wantErr string
	}{
		{
			name:    "missing output dir",
			cfg:     Config{TmpDir: "/tmp"},
			files:   []string{"test.csv"},
			wantErr: "output directory required",
		},
		{
			name:    "missing temp dir",
			cfg:     Config{OutDir: "/tmp/out"},
			files:   []string{"test.csv"},
			wantErr: "temp directory required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Build(ctx, tt.cfg, tt.files)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestBuildWithSchemaValidation(t *testing.T) {
	ctx := context.Background()

	err := BuildWithSchema(ctx, Config{}, nil, SchemaConfig{})
	if err == nil {
		t.Fatal("expected error for empty config")
	}
}

func TestBuildFromS3Validation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		cfg     S3Config
		wantErr string
	}{
		{
			name:    "missing manifest URI",
			cfg:     S3Config{Config: Config{OutDir: "/out", TmpDir: "/tmp"}},
			wantErr: "manifest URI required",
		},
		{
			name:    "missing output dir",
			cfg:     S3Config{ManifestURI: "s3://bucket/manifest.json", Config: Config{TmpDir: "/tmp"}},
			wantErr: "output directory required",
		},
		{
			name:    "missing temp dir",
			cfg:     S3Config{ManifestURI: "s3://bucket/manifest.json", Config: Config{OutDir: "/out"}},
			wantErr: "temp directory required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := BuildFromS3(ctx, tt.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// createTestInventory creates a test CSV inventory file.
func createTestInventory(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	return path
}

func TestBuildSimple(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size
a/file1.txt,100
b/file2.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
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
		"depth_offsets.u64",
		"depth_positions.u64",
		"mph.bin",
		"mph_fp.u64",
		"mph_pos.u64",
		"prefix_blob.bin",
		"prefix_offsets.u64",
	}
	for _, name := range expectedFiles {
		path := filepath.Join(outDir, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file %q not found: %v", name, err)
		}
	}
}

func TestBuildWithTiers(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size,StorageClass
a/file1.txt,100,STANDARD
b/file2.txt,200,GLACIER
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:     outDir,
		TmpDir:     sortDir,
		ChunkSize:  100,
		TrackTiers: true,
	}

	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify tier_stats directory was created
	tierStatsDir := filepath.Join(outDir, "tier_stats")
	if _, err := os.Stat(tierStatsDir); err != nil {
		t.Errorf("tier_stats directory not created: %v", err)
	}

	// Verify tiers.json was created
	tiersManifest := filepath.Join(outDir, "tiers.json")
	if _, err := os.Stat(tiersManifest); err != nil {
		t.Errorf("tiers.json not created: %v", err)
	}
}

func TestBuildWithSchema(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Headerless CSV (like AWS S3 inventory)
	csv := `a/file1.txt,100
b/file2.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}
	schema := SchemaConfig{
		KeyCol:        0,
		SizeCol:       1,
		StorageCol:    -1,
		AccessTierCol: -1,
	}

	err := BuildWithSchema(context.Background(), cfg, []string{invPath}, schema)
	if err != nil {
		t.Fatalf("BuildWithSchema failed: %v", err)
	}

	// Verify output
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); err != nil {
		t.Errorf("manifest.json not created: %v", err)
	}
}

func TestBuildMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv1 := `Key,Size
a/file1.txt,100
`
	csv2 := `Key,Size
b/file2.txt,200
`
	invPath1 := createTestInventory(t, tmpDir, "inv1.csv", csv1)
	invPath2 := createTestInventory(t, tmpDir, "inv2.csv", csv2)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	err := Build(context.Background(), cfg, []string{invPath1, invPath2})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
}

func TestBuildNonexistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	err := Build(context.Background(), cfg, []string{"/nonexistent/file.csv"})
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestBuildCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size
a/file1.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Build(ctx, cfg, []string{invPath})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestBuildDefaultChunkSize(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size
a/file1.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	// ChunkSize = 0 should use default
	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 0,
	}

	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
}

func TestBuildAtomicFailure(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a file where output dir should be (to cause rename failure on some systems)
	// This tests the cleanup behavior
	csv := `Key,Size
a/file1.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	// First build should succeed
	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	// Second build should also succeed (replaces existing)
	err = Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
	}

	// Temp directory should be cleaned up
	tmpOutDir := outDir + ".tmp"
	if _, err := os.Stat(tmpOutDir); !os.IsNotExist(err) {
		t.Error("temp output dir should be cleaned up")
	}
}

func TestBuildResume(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size
a/file1.txt,100
b/file2.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	// First build should succeed
	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	// Get modification time of manifest
	manifestPath := filepath.Join(outDir, "manifest.json")
	info1, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	modTime1 := info1.ModTime()

	// Second build should skip (resume from completed build)
	err = Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
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

func TestBuildResumeAfterCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		t.Fatal(err)
	}

	csv := `Key,Size
a/file1.txt,100
b/file2.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	// First build should succeed
	err := Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("First build failed: %v", err)
	}

	// Corrupt a file
	subtreeEndPath := filepath.Join(outDir, "subtree_end.u64")
	if err := os.Truncate(subtreeEndPath, 10); err != nil {
		t.Fatalf("truncate file: %v", err)
	}

	// Get modification time of manifest
	manifestPath := filepath.Join(outDir, "manifest.json")
	info1, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	modTime1 := info1.ModTime()

	// Second build should rebuild (validation fails due to corruption)
	err = Build(context.Background(), cfg, []string{invPath})
	if err != nil {
		t.Fatalf("Second build failed: %v", err)
	}

	// Manifest should be modified (build was not skipped)
	info2, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest after second build: %v", err)
	}
	modTime2 := info2.ModTime()

	if modTime1.Equal(modTime2) {
		t.Errorf("manifest was NOT modified - expected rebuild after corruption")
	}
}

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
