package sqliteagg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestNormalizedAggregator_Basic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := DefaultNormalizedConfig(dbPath)
	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	// Begin chunk
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	// Add objects
	objects := []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"data/2024/01/file1.txt", 100, tiers.Standard},
		{"data/2024/01/file2.txt", 200, tiers.Standard},
		{"data/2024/02/file3.txt", 300, tiers.StandardIA},
		{"logs/app.log", 50, tiers.GlacierFR},
	}

	for _, obj := range objects {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s): %v", obj.key, err)
		}
	}

	// Mark chunk done
	if err := agg.MarkChunkDone("chunk-1"); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}

	// Commit
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Finalize (create indexes)
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify prefix count
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	// Expected: "" (root), "data/", "data/2024/", "data/2024/01/", "data/2024/02/", "logs/"
	if count != 6 {
		t.Errorf("PrefixCount = %d, want 6", count)
	}

	// Verify max depth
	maxDepth, err := agg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth: %v", err)
	}
	if maxDepth != 3 { // data/2024/01/ and data/2024/02/ are depth 3
		t.Errorf("MaxDepth = %d, want 3", maxDepth)
	}

	// Verify chunk is marked done
	done, err := agg.ChunkDone("chunk-1")
	if err != nil {
		t.Fatalf("ChunkDone: %v", err)
	}
	if !done {
		t.Error("chunk-1 should be marked as done")
	}

	done, err = agg.ChunkDone("chunk-2")
	if err != nil {
		t.Fatalf("ChunkDone: %v", err)
	}
	if done {
		t.Error("chunk-2 should not be marked as done")
	}
}

func TestNormalizedAggregator_IteratePrefixes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := DefaultNormalizedConfig(dbPath)
	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	// Begin chunk and add objects
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	objects := []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"a/b/file.txt", 100, tiers.Standard},
		{"a/c/file.txt", 200, tiers.StandardIA},
	}

	for _, obj := range objects {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s): %v", obj.key, err)
		}
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Iterate prefixes and verify order + values
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	expected := []struct {
		prefix     string
		depth      int
		totalCount uint64
		totalBytes uint64
	}{
		{"", 0, 2, 300},    // root: both objects
		{"a/", 1, 2, 300},  // a/: both objects
		{"a/b/", 2, 1, 100}, // a/b/: first object only
		{"a/c/", 2, 1, 200}, // a/c/: second object only
	}

	i := 0
	for iter.Next() {
		if i >= len(expected) {
			t.Fatalf("too many prefixes, expected %d", len(expected))
		}

		row := iter.Row()
		exp := expected[i]

		if row.Prefix != exp.prefix {
			t.Errorf("row %d: Prefix = %q, want %q", i, row.Prefix, exp.prefix)
		}
		if row.Depth != exp.depth {
			t.Errorf("row %d: Depth = %d, want %d", i, row.Depth, exp.depth)
		}
		if row.TotalCount != exp.totalCount {
			t.Errorf("row %d: TotalCount = %d, want %d", i, row.TotalCount, exp.totalCount)
		}
		if row.TotalBytes != exp.totalBytes {
			t.Errorf("row %d: TotalBytes = %d, want %d", i, row.TotalBytes, exp.totalBytes)
		}

		i++
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if i != len(expected) {
		t.Errorf("got %d prefixes, want %d", i, len(expected))
	}
}

func TestNormalizedAggregator_MultiChunk(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := DefaultNormalizedConfig(dbPath)
	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	// Chunk 1
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("data/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.MarkChunkDone("chunk-1"); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Chunk 2 - same prefix, should aggregate
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("data/file2.txt", 200, tiers.StandardIA); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.MarkChunkDone("chunk-2"); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify aggregation
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	var foundData bool
	for iter.Next() {
		row := iter.Row()
		if row.Prefix == "data/" {
			foundData = true
			if row.TotalCount != 2 {
				t.Errorf("data/ TotalCount = %d, want 2", row.TotalCount)
			}
			if row.TotalBytes != 300 {
				t.Errorf("data/ TotalBytes = %d, want 300", row.TotalBytes)
			}
		}
	}

	if !foundData {
		t.Error("data/ prefix not found")
	}
}

func TestNormalizedAggregator_Rollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := DefaultNormalizedConfig(dbPath)
	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	// Successful chunk
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("data/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.MarkChunkDone("chunk-1"); err != nil {
		t.Fatalf("MarkChunkDone: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Rollback chunk
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("logs/file2.txt", 200, tiers.StandardIA); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Only chunk-1 data should exist
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	// Only "" (root) and "data/" from chunk 1
	if count != 2 {
		t.Errorf("PrefixCount = %d, want 2", count)
	}
}

func TestNormalizedAggregator_PresentTiers(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	cfg := DefaultNormalizedConfig(dbPath)
	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	// Add objects in different tiers
	if err := agg.AddObject("standard.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("glacier.txt", 200, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject: %v", err)
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers: %v", err)
	}

	// Should have Standard and Glacier
	if len(present) != 2 {
		t.Errorf("PresentTiers = %v, want 2 tiers", present)
	}

	hasStandard := false
	hasGlacierFR := false
	for _, id := range present {
		if id == tiers.Standard {
			hasStandard = true
		}
		if id == tiers.GlacierFR {
			hasGlacierFR = true
		}
	}

	if !hasStandard {
		t.Error("Standard tier not present")
	}
	if !hasGlacierFR {
		t.Error("GlacierFR tier not present")
	}
}

func TestNormalizedAggregator_MemorySpillover(t *testing.T) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		t.Skip("set S3INV_LONG_BENCH=1 to run memory spillover test")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Very low memory limit to force spillover
	cfg := DefaultNormalizedConfig(dbPath)
	cfg.MemoryLimitMB = 1 // 1MB limit

	agg, err := NewNormalizedAggregator(cfg)
	if err != nil {
		t.Fatalf("NewNormalizedAggregator: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	// Add enough objects to trigger spillover
	for i := 0; i < 50000; i++ {
		key := filepath.Join("data", "prefix"+string(rune('a'+i%26)), "file.txt")
		if err := agg.AddObject(key, uint64(i), tiers.Standard); err != nil {
			t.Fatalf("AddObject: %v", err)
		}
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Verify we have data
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	if count == 0 {
		t.Error("no prefixes after spillover")
	}
	t.Logf("Prefix count after spillover: %d", count)
}
