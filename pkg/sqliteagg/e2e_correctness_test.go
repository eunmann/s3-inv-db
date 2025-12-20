package sqliteagg

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
SQLite layer correctness tests.

These tests verify that the MemoryAggregator correctly accumulates prefix stats
before they are written to SQLite and converted to an index.

Uses the same synthetic inventory as pkg/indexbuild/e2e_correctness_test.go:
  root/                          # 6 objects, 600 bytes total
  ├── file1.txt                  # 100 bytes, Standard
  ├── a/                         # 3 objects, 350 bytes
  │   ├── file2.txt              # 50 bytes, Standard
  │   └── b/                     # 2 objects, 300 bytes
  │       ├── file3.txt          # 100 bytes, GlacierFR
  │       └── file4.txt          # 200 bytes, Standard
  └── c/                         # 2 objects, 150 bytes
      ├── file5.txt              # 75 bytes, Standard
      └── file6.txt              # 75 bytes, DeepArchive
*/

// testObject represents a single S3 object in the synthetic inventory.
type testObject struct {
	key    string
	size   uint64
	tierID tiers.ID
}

// syntheticInventory is the hand-crafted test dataset.
var syntheticInventory = []testObject{
	{"file1.txt", 100, tiers.Standard},
	{"a/file2.txt", 50, tiers.Standard},
	{"a/b/file3.txt", 100, tiers.GlacierFR},
	{"a/b/file4.txt", 200, tiers.Standard},
	{"c/file5.txt", 75, tiers.Standard},
	{"c/file6.txt", 75, tiers.DeepArchive},
}

// expectedPrefixStats holds the expected in-memory stats for each prefix.
type expectedPrefixStats struct {
	prefix     string
	depth      int
	count      uint64
	bytes      uint64
	tierCounts map[tiers.ID]uint64
	tierBytes  map[tiers.ID]uint64
}

// expectedStats contains known-correct stats for all prefixes.
var expectedStats = []expectedPrefixStats{
	{
		prefix: "", depth: 0, count: 6, bytes: 600,
		tierCounts: map[tiers.ID]uint64{tiers.Standard: 4, tiers.GlacierFR: 1, tiers.DeepArchive: 1},
		tierBytes:  map[tiers.ID]uint64{tiers.Standard: 425, tiers.GlacierFR: 100, tiers.DeepArchive: 75},
	},
	{
		prefix: "a/", depth: 1, count: 3, bytes: 350,
		tierCounts: map[tiers.ID]uint64{tiers.Standard: 2, tiers.GlacierFR: 1},
		tierBytes:  map[tiers.ID]uint64{tiers.Standard: 250, tiers.GlacierFR: 100},
	},
	{
		prefix: "a/b/", depth: 2, count: 2, bytes: 300,
		tierCounts: map[tiers.ID]uint64{tiers.Standard: 1, tiers.GlacierFR: 1},
		tierBytes:  map[tiers.ID]uint64{tiers.Standard: 200, tiers.GlacierFR: 100},
	},
	{
		prefix: "c/", depth: 1, count: 2, bytes: 150,
		tierCounts: map[tiers.ID]uint64{tiers.Standard: 1, tiers.DeepArchive: 1},
		tierBytes:  map[tiers.ID]uint64{tiers.Standard: 75, tiers.DeepArchive: 75},
	},
}

// TestMemoryAggregator_PrefixStats validates in-memory prefix accumulation.
func TestMemoryAggregator_PrefixStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg := NewMemoryAggregator(DefaultMemoryAggregatorConfig(dbPath))

	// Add all objects
	for _, obj := range syntheticInventory {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}

	// Validate prefix count
	if agg.PrefixCount() != len(expectedStats) {
		t.Errorf("PrefixCount() = %d, want %d", agg.PrefixCount(), len(expectedStats))
	}

	// Access internal map for validation (test-only)
	for _, exp := range expectedStats {
		t.Run("prefix="+quotePrefixForTest(exp.prefix), func(t *testing.T) {
			stats, ok := agg.prefixes[exp.prefix]
			if !ok {
				t.Fatalf("prefix %q not found in aggregator", exp.prefix)
			}

			if stats.Depth != exp.depth {
				t.Errorf("Depth = %d, want %d", stats.Depth, exp.depth)
			}
			if stats.TotalCount != exp.count {
				t.Errorf("TotalCount = %d, want %d", stats.TotalCount, exp.count)
			}
			if stats.TotalBytes != exp.bytes {
				t.Errorf("TotalBytes = %d, want %d", stats.TotalBytes, exp.bytes)
			}

			// Validate tier counts
			for tierID, wantCount := range exp.tierCounts {
				if stats.TierCounts[tierID] != wantCount {
					t.Errorf("TierCounts[%d] = %d, want %d", tierID, stats.TierCounts[tierID], wantCount)
				}
			}

			// Validate tier bytes
			for tierID, wantBytes := range exp.tierBytes {
				if stats.TierBytes[tierID] != wantBytes {
					t.Errorf("TierBytes[%d] = %d, want %d", tierID, stats.TierBytes[tierID], wantBytes)
				}
			}
		})
	}
}

// TestMemoryAggregator_SQLiteRoundtrip validates that data survives SQLite write/read.
func TestMemoryAggregator_SQLiteRoundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Aggregate and write to SQLite
	agg := NewMemoryAggregator(DefaultMemoryAggregatorConfig(dbPath))
	for _, obj := range syntheticInventory {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	// Reopen SQLite and validate via Aggregator (read-only)
	cfg := DefaultConfig(dbPath)
	sqliteAgg, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sqliteAgg.Close()

	// Validate prefix count
	count, err := sqliteAgg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount failed: %v", err)
	}
	if count != uint64(len(expectedStats)) {
		t.Errorf("SQLite PrefixCount = %d, want %d", count, len(expectedStats))
	}

	// Validate via iterator
	iter, err := sqliteAgg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
	}
	defer iter.Close()

	seen := make(map[string]bool)
	for iter.Next() {
		row := iter.Row()
		seen[row.Prefix] = true

		// Find expected stats for this prefix
		var exp *expectedPrefixStats
		for i := range expectedStats {
			if expectedStats[i].prefix == row.Prefix {
				exp = &expectedStats[i]
				break
			}
		}
		if exp == nil {
			t.Errorf("unexpected prefix in SQLite: %q", row.Prefix)
			continue
		}

		if row.Depth != exp.depth {
			t.Errorf("prefix %q: Depth = %d, want %d", row.Prefix, row.Depth, exp.depth)
		}
		if row.TotalCount != exp.count {
			t.Errorf("prefix %q: TotalCount = %d, want %d", row.Prefix, row.TotalCount, exp.count)
		}
		if row.TotalBytes != exp.bytes {
			t.Errorf("prefix %q: TotalBytes = %d, want %d", row.Prefix, row.TotalBytes, exp.bytes)
		}

		// Validate tier counts
		for tierID, wantCount := range exp.tierCounts {
			if row.TierCounts[tierID] != wantCount {
				t.Errorf("prefix %q: TierCounts[%d] = %d, want %d",
					row.Prefix, tierID, row.TierCounts[tierID], wantCount)
			}
		}

		// Validate tier bytes
		for tierID, wantBytes := range exp.tierBytes {
			if row.TierBytes[tierID] != wantBytes {
				t.Errorf("prefix %q: TierBytes[%d] = %d, want %d",
					row.Prefix, tierID, row.TierBytes[tierID], wantBytes)
			}
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	// Verify all expected prefixes were seen
	for _, exp := range expectedStats {
		if !seen[exp.prefix] {
			t.Errorf("missing prefix in SQLite: %q", exp.prefix)
		}
	}
}

// TestMemoryAggregator_MaxDepth validates max depth tracking.
func TestMemoryAggregator_MaxDepth(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg := NewMemoryAggregator(DefaultMemoryAggregatorConfig(dbPath))
	for _, obj := range syntheticInventory {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	cfg := DefaultConfig(dbPath)
	sqliteAgg, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sqliteAgg.Close()

	maxDepth, err := sqliteAgg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth failed: %v", err)
	}
	// Our deepest prefix is "a/b/" at depth 2
	if maxDepth != 2 {
		t.Errorf("MaxDepth = %d, want 2", maxDepth)
	}
}

// TestMemoryAggregator_PresentTiers validates tier tracking.
func TestMemoryAggregator_PresentTiers(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg := NewMemoryAggregator(DefaultMemoryAggregatorConfig(dbPath))
	for _, obj := range syntheticInventory {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	cfg := DefaultConfig(dbPath)
	sqliteAgg, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer sqliteAgg.Close()

	presentTiers, err := sqliteAgg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers failed: %v", err)
	}

	// We expect 3 tiers: Standard, GlacierFR, DeepArchive
	expected := map[tiers.ID]bool{
		tiers.Standard:    true,
		tiers.GlacierFR:   true,
		tiers.DeepArchive: true,
	}

	if len(presentTiers) != len(expected) {
		t.Errorf("PresentTiers count = %d, want %d", len(presentTiers), len(expected))
	}

	for _, tierID := range presentTiers {
		if !expected[tierID] {
			t.Errorf("unexpected tier in PresentTiers: %d", tierID)
		}
	}
}

// quotePrefixForTest formats a prefix for test names.
func quotePrefixForTest(prefix string) string {
	if prefix == "" {
		return "root"
	}
	return prefix
}
