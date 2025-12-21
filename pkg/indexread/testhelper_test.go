package indexread

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
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

	// Use extsort aggregator to build index
	agg := extsort.NewAggregator(len(keys), 0)

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100) // Default size varies by index
		if sizes != nil && i < len(sizes) {
			size = sizes[i]
		}
		agg.AddObject(key, size, tiers.Standard)
	}

	// Drain to rows and sort
	rows := agg.Drain()

	// Build index using streaming builder
	builder, err := extsort.NewIndexBuilder(outDir, "")
	if err != nil {
		return fmt.Errorf("create index builder: %w", err)
	}

	// Sort rows by prefix for proper preorder
	extsort.SortPrefixRows(rows)

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return fmt.Errorf("add row: %w", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		return fmt.Errorf("finalize: %w", err)
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
		return errors.New("CSV must have at least header and one data row")
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
	for i := range len(s) {
		if s[i] == '\n' {
			line := s[start:i]
			if line != "" && line[len(line)-1] == '\r' {
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
	for i := range len(line) {
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

// testObject represents a single object for test index building.
type testObject struct {
	Key    string
	Size   uint64
	TierID tiers.ID
}

// buildIndexWithTiers creates an index from objects with tier data.
func buildIndexWithTiers(t *testing.T, outDir string, objects []testObject) error {
	t.Helper()

	// Use extsort aggregator to build index
	agg := extsort.NewAggregator(len(objects), 0)

	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}

	// Drain to rows and sort
	rows := agg.Drain()

	// Build index using streaming builder
	builder, err := extsort.NewIndexBuilder(outDir, "")
	if err != nil {
		return fmt.Errorf("create index builder: %w", err)
	}

	// Sort rows by prefix for proper preorder
	extsort.SortPrefixRows(rows)

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return fmt.Errorf("add row: %w", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}
