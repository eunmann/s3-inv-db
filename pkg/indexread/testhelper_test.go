package indexread

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// buildIndexFromKeys creates an index from a slice of keys for testing.
// Keys should be S3-style object keys (e.g., "a/b/file.txt").
// Sizes are assigned based on key index.
func buildIndexFromKeys(t *testing.T, outDir string, keys []string) error {
	t.Helper()
	return buildIndexFromKeysWithSizes(t, outDir, keys, nil)
}

// buildIndexFromKeysWithSizes creates an index from keys with specific sizes.
func buildIndexFromKeysWithSizes(t *testing.T, outDir string, keys []string, sizes []uint64) error {
	t.Helper()

	dbPath := outDir + ".db"
	cfg := sqliteagg.DefaultConfig(dbPath)

	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		return fmt.Errorf("open SQLite: %w", err)
	}
	defer agg.Close()
	defer os.Remove(dbPath)

	if err := agg.BeginChunk(); err != nil {
		return fmt.Errorf("begin chunk: %w", err)
	}

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100) // Default size varies by index
		if sizes != nil && i < len(sizes) {
			size = sizes[i]
		}
		if err := agg.AddObject(key, size, tiers.Standard); err != nil {
			return fmt.Errorf("add object %q: %w", key, err)
		}
	}

	if err := agg.MarkChunkDone("test-chunk"); err != nil {
		return fmt.Errorf("mark chunk done: %w", err)
	}
	if err := agg.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	buildCfg := indexbuild.SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: cfg,
	}

	if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
		return fmt.Errorf("build index: %w", err)
	}

	return nil
}

// buildIndexFromCSV creates an index from a CSV content string.
// CSV should have columns Key,Size (no StorageClass).
func buildIndexFromCSV(t *testing.T, outDir, csv string) error {
	t.Helper()

	// Parse CSV
	lines := splitLines(csv)
	if len(lines) < 2 {
		return fmt.Errorf("CSV must have at least header and one data row")
	}

	// Skip header
	var keys []string
	var sizes []uint64
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		parts := splitCSVLine(line)
		if len(parts) < 2 {
			continue
		}
		keys = append(keys, parts[0])
		var size uint64
		fmt.Sscanf(parts[1], "%d", &size)
		sizes = append(sizes, size)
	}

	return buildIndexFromKeysWithSizes(t, outDir, keys, sizes)
}

// splitLines splits a string into lines, handling both \n and \r\n.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// splitCSVLine splits a CSV line by commas (simple, no quoting).
func splitCSVLine(line string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(line); i++ {
		if line[i] == ',' {
			parts = append(parts, line[start:i])
			start = i + 1
		}
	}
	parts = append(parts, line[start:])
	return parts
}

// setupTestIndex creates a test index directory.
func setupTestIndex(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "index")
}
