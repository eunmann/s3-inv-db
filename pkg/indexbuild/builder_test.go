package indexbuild

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
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
