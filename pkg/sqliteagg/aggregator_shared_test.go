package sqliteagg

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// ChunkAggregatorFactory creates a ChunkAggregator for testing.
// The factory receives a testing.TB to access TempDir() and logging.
type ChunkAggregatorFactory func(tb testing.TB) ChunkAggregator

// aggregatorImplementations returns all implementations to test.
// Each implementation is identified by name and factory.
func aggregatorImplementations() []struct {
	name    string
	factory ChunkAggregatorFactory
} {
	return []struct {
		name    string
		factory ChunkAggregatorFactory
	}{
		{
			name: "Standard",
			factory: func(tb testing.TB) ChunkAggregator {
				tb.Helper()
				dbPath := filepath.Join(tb.TempDir(), "test.db")
				cfg := DefaultConfig(dbPath)
				cfg.Synchronous = "OFF"
				cfg.BulkWriteMode = true
				agg, err := Open(cfg)
				if err != nil {
					tb.Fatalf("Open failed: %v", err)
				}
				return agg
			},
		},
	}
}

// testObjects returns a standard set of test objects for shared tests.
func testObjects() []struct {
	key    string
	size   uint64
	tierID tiers.ID
} {
	return []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"a/file1.txt", 100, tiers.Standard},
		{"a/file2.txt", 200, tiers.Standard},
		{"a/b/file3.txt", 300, tiers.GlacierFR},
		{"b/file4.txt", 400, tiers.StandardIA},
	}
}

// --- Shared Test Scenarios ---

// testChunkAggregatorBasic tests basic chunk workflow.
func testChunkAggregatorBasic(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// Begin chunk
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	// Add objects
	for _, obj := range testObjects() {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s): %v", obj.key, err)
		}
	}

	// Commit
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify prefix count: "", "a/", "a/b/", "b/"
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	if count != 4 {
		t.Errorf("PrefixCount = %d, want 4", count)
	}

	// Verify max depth
	maxDepth, err := agg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth: %v", err)
	}
	if maxDepth != 2 {
		t.Errorf("MaxDepth = %d, want 2", maxDepth)
	}
}

// testChunkAggregatorRollback tests rollback behavior.
func testChunkAggregatorRollback(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// Begin, add data, but rollback
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify no prefixes were added
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	if count != 0 {
		t.Errorf("PrefixCount = %d, want 0", count)
	}
}

// testChunkAggregatorIterate tests prefix iteration.
func testChunkAggregatorIterate(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// Add objects
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("b/file2.txt", 200, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Iterate and verify order
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	expectedPrefixes := []string{"", "a/", "b/"}
	var gotPrefixes []string

	for iter.Next() {
		row := iter.Row()
		gotPrefixes = append(gotPrefixes, row.Prefix)
	}

	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}

	if len(gotPrefixes) != len(expectedPrefixes) {
		t.Fatalf("got %d prefixes, want %d: %v", len(gotPrefixes), len(expectedPrefixes), gotPrefixes)
	}

	for i, prefix := range gotPrefixes {
		if prefix != expectedPrefixes[i] {
			t.Errorf("prefix[%d] = %q, want %q", i, prefix, expectedPrefixes[i])
		}
	}
}

// testChunkAggregatorAggregation tests prefix aggregation.
func testChunkAggregatorAggregation(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// Add objects with same prefix multiple times
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("a/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("a/file3.txt", 50, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify aggregation
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	for iter.Next() {
		row := iter.Row()
		if row.Prefix == "a/" {
			if row.TotalCount != 3 {
				t.Errorf("a/ TotalCount = %d, want 3", row.TotalCount)
			}
			if row.TotalBytes != 350 {
				t.Errorf("a/ TotalBytes = %d, want 350", row.TotalBytes)
			}
			if row.TierCounts[tiers.Standard] != 2 {
				t.Errorf("a/ Standard count = %d, want 2", row.TierCounts[tiers.Standard])
			}
			if row.TierBytes[tiers.Standard] != 300 {
				t.Errorf("a/ Standard bytes = %d, want 300", row.TierBytes[tiers.Standard])
			}
			if row.TierCounts[tiers.GlacierFR] != 1 {
				t.Errorf("a/ Glacier count = %d, want 1", row.TierCounts[tiers.GlacierFR])
			}
			if row.TierBytes[tiers.GlacierFR] != 50 {
				t.Errorf("a/ Glacier bytes = %d, want 50", row.TierBytes[tiers.GlacierFR])
			}
		}
	}
}

// testChunkAggregatorPresentTiers tests present tiers detection.
func testChunkAggregatorPresentTiers(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("b/file2.txt", 200, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("c/file3.txt", 300, tiers.DeepArchive); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers: %v", err)
	}

	if len(present) != 3 {
		t.Errorf("len(present) = %d, want 3", len(present))
	}

	tierSet := make(map[tiers.ID]bool)
	for _, id := range present {
		tierSet[id] = true
	}

	if !tierSet[tiers.Standard] {
		t.Error("Standard tier not present")
	}
	if !tierSet[tiers.GlacierFR] {
		t.Error("GlacierFR tier not present")
	}
	if !tierSet[tiers.DeepArchive] {
		t.Error("DeepArchive tier not present")
	}
}

// testChunkAggregatorMultiChunk tests multi-chunk aggregation.
func testChunkAggregatorMultiChunk(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// Chunk 1
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("a/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Chunk 2 - same prefix, different tier
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}
	if err := agg.AddObject("a/file3.txt", 300, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("b/file1.txt", 400, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify combined stats
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	prefixStats := make(map[string]PrefixRow)
	for iter.Next() {
		row := iter.Row()
		prefixStats[row.Prefix] = row
	}

	// Root should have 4 objects, 1000 bytes
	root := prefixStats[""]
	if root.TotalCount != 4 {
		t.Errorf("root TotalCount = %d, want 4", root.TotalCount)
	}
	if root.TotalBytes != 1000 {
		t.Errorf("root TotalBytes = %d, want 1000", root.TotalBytes)
	}

	// a/ should have combined stats from both chunks
	aPrefix := prefixStats["a/"]
	if aPrefix.TotalCount != 3 {
		t.Errorf("a/ TotalCount = %d, want 3", aPrefix.TotalCount)
	}
	if aPrefix.TotalBytes != 600 {
		t.Errorf("a/ TotalBytes = %d, want 600", aPrefix.TotalBytes)
	}
}

// testChunkAggregatorDeepHierarchy tests deep prefix hierarchies.
func testChunkAggregatorDeepHierarchy(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	if err := agg.AddObject("a/b/c/d/e/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("a/b/c/d/e/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.AddObject("a/b/c/other.txt", 50, tiers.Standard); err != nil {
		t.Fatalf("AddObject: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify max depth
	maxDepth, err := agg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth: %v", err)
	}
	if maxDepth != 5 {
		t.Errorf("MaxDepth = %d, want 5", maxDepth)
	}

	// Verify prefix stats
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	prefixStats := make(map[string]PrefixRow)
	for iter.Next() {
		row := iter.Row()
		prefixStats[row.Prefix] = row
	}

	// a/b/c/ should have 3 objects
	if prefixStats["a/b/c/"].TotalCount != 3 {
		t.Errorf("a/b/c/ TotalCount = %d, want 3", prefixStats["a/b/c/"].TotalCount)
	}

	// a/b/c/d/e/ should have 2 objects
	if prefixStats["a/b/c/d/e/"].TotalCount != 2 {
		t.Errorf("a/b/c/d/e/ TotalCount = %d, want 2", prefixStats["a/b/c/d/e/"].TotalCount)
	}

	// Verify depths
	if prefixStats["a/"].Depth != 1 {
		t.Errorf("a/ depth = %d, want 1", prefixStats["a/"].Depth)
	}
	if prefixStats["a/b/c/d/e/"].Depth != 5 {
		t.Errorf("a/b/c/d/e/ depth = %d, want 5", prefixStats["a/b/c/d/e/"].Depth)
	}
}

// testChunkAggregatorEmpty tests empty database handling.
func testChunkAggregatorEmpty(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	// No data added
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount: %v", err)
	}
	if count != 0 {
		t.Errorf("PrefixCount = %d, want 0", count)
	}

	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers: %v", err)
	}
	if len(present) != 0 {
		t.Errorf("len(present) = %d, want 0", len(present))
	}
}

// testChunkAggregatorAllTiers tests handling of all supported tiers.
func testChunkAggregatorAllTiers(t *testing.T, factory ChunkAggregatorFactory) {
	t.Helper()

	agg := factory(t)
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk: %v", err)
	}

	allTiers := []tiers.ID{
		tiers.Standard,
		tiers.StandardIA,
		tiers.OneZoneIA,
		tiers.GlacierIR,
		tiers.GlacierFR,
		tiers.DeepArchive,
		tiers.ReducedRedundancy,
		tiers.ITFrequent,
		tiers.ITInfrequent,
	}

	for i, tierID := range allTiers {
		key := fmt.Sprintf("data/tier%d.txt", tierID)
		if err := agg.AddObject(key, uint64((i+1)*100), tierID); err != nil {
			t.Fatalf("AddObject(%s, tier=%d): %v", key, tierID, err)
		}
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers: %v", err)
	}
	if len(present) != len(allTiers) {
		t.Errorf("len(present) = %d, want %d", len(present), len(allTiers))
	}

	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes: %v", err)
	}
	defer iter.Close()

	for iter.Next() {
		row := iter.Row()
		if row.Prefix == "data/" {
			for _, tierID := range allTiers {
				if row.TierCounts[tierID] != 1 {
					t.Errorf("data/ tier %d count = %d, want 1", tierID, row.TierCounts[tierID])
				}
			}
		}
	}
}

// --- Shared Benchmark Helpers ---

// BenchmarkAggregatorFactory creates a ChunkAggregator for benchmarks.
type BenchmarkAggregatorFactory func(b *testing.B, dbPath string) ChunkAggregator

// benchmarkAggregatorFactories returns factories for benchmarking.
func benchmarkAggregatorFactories() []struct {
	name    string
	factory BenchmarkAggregatorFactory
} {
	return []struct {
		name    string
		factory BenchmarkAggregatorFactory
	}{
		{
			name: "Standard",
			factory: func(b *testing.B, dbPath string) ChunkAggregator {
				b.Helper()
				cfg := DefaultConfig(dbPath)
				cfg.Synchronous = "OFF"
				cfg.BulkWriteMode = true
				agg, err := Open(cfg)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				return agg
			},
		},
	}
}

// benchmarkAggregatorIngest benchmarks object ingestion.
func benchmarkAggregatorIngest(b *testing.B, factory BenchmarkAggregatorFactory, objects []benchutil.FakeObject) {
	b.Helper()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(b.TempDir(), "bench.db")
		b.StartTimer()

		agg := factory(b, dbPath)

		if err := agg.BeginChunk(); err != nil {
			b.Fatalf("BeginChunk: %v", err)
		}
		for _, obj := range objects {
			if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
				b.Fatalf("AddObject: %v", err)
			}
		}
		if err := agg.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}

		b.StopTimer()
		if i == b.N-1 {
			prefixCount, _ := agg.PrefixCount()
			elapsed := b.Elapsed()
			objPerSec := float64(len(objects)) / elapsed.Seconds()
			b.Logf("prefixes=%d obj/s=%.0f", prefixCount, objPerSec)
		}
		agg.Close()
	}
}

// benchmarkAggregatorIterate benchmarks prefix iteration.
func benchmarkAggregatorIterate(b *testing.B, factory BenchmarkAggregatorFactory, numObjects int) {
	b.Helper()

	// Setup: populate database
	dbPath := filepath.Join(b.TempDir(), "bench.db")
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	agg := factory(b, dbPath)
	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk: %v", err)
	}
	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject: %v", err)
		}
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		it, err := agg.IteratePrefixes()
		if err != nil {
			b.Fatalf("IteratePrefixes: %v", err)
		}

		count := 0
		for it.Next() {
			_ = it.Row()
			count++
		}
		if err := it.Err(); err != nil {
			b.Fatalf("Iterator error: %v", err)
		}
		it.Close()

		if i == b.N-1 {
			b.Logf("iterated %d prefixes", count)
		}
	}

	b.StopTimer()
	agg.Close()
}
