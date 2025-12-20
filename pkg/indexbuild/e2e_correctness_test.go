package indexbuild

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
End-to-end correctness tests for prefix statistics.

These tests use a hand-crafted synthetic inventory with known prefix stats
to validate the entire pipeline: MemoryAggregator → SQLite → Trie → Index.

Test Inventory Layout:
  root/                          # 6 objects, 600 bytes total
  ├── file1.txt                  # 100 bytes, Standard
  ├── a/                         # 3 objects, 350 bytes
  │   ├── file2.txt              # 50 bytes, Standard
  │   └── b/                     # 2 objects, 300 bytes
  │       ├── file3.txt          # 100 bytes, Glacier
  │       └── file4.txt          # 200 bytes, Standard
  └── c/                         # 2 objects, 150 bytes
      ├── file5.txt              # 75 bytes, Standard
      └── file6.txt              # 75 bytes, GlacierDeepArchive

Expected Prefixes:
  - ""      (root): 6 objects, 600 bytes
  - "a/"          : 3 objects, 350 bytes
  - "a/b/"        : 2 objects, 300 bytes
  - "c/"          : 2 objects, 150 bytes
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

// expectedStats holds the expected stats for each prefix.
type expectedStats struct {
	prefix      string
	depth       uint32
	objectCount uint64
	totalBytes  uint64
}

// expectedPrefixStats contains known-correct stats for all prefixes.
var expectedPrefixStats = []expectedStats{
	{"", 0, 6, 600},     // root: all 6 objects
	{"a/", 1, 3, 350},   // a/: file2 + file3 + file4
	{"a/b/", 2, 2, 300}, // a/b/: file3 + file4
	{"c/", 1, 2, 150},   // c/: file5 + file6
}

// expectedTierStats holds per-tier expectations for validation.
type expectedTierStats struct {
	prefix   string
	tierName string
	count    uint64
	bytes    uint64
}

// expectedTierBreakdown contains known-correct tier stats.
var expectedTierBreakdown = []expectedTierStats{
	// Root has objects in 3 tiers
	{"", "STANDARD", 4, 425},     // file1+file2+file4+file5
	{"", "GLACIER", 1, 100},      // file3
	{"", "DEEP_ARCHIVE", 1, 75},  // file6
	// a/ has Standard and Glacier
	{"a/", "STANDARD", 2, 250},   // file2+file4
	{"a/", "GLACIER", 1, 100},    // file3
	// a/b/ has Standard and Glacier
	{"a/b/", "STANDARD", 1, 200}, // file4
	{"a/b/", "GLACIER", 1, 100},  // file3
	// c/ has Standard and DeepArchive
	{"c/", "STANDARD", 1, 75},    // file5
	{"c/", "DEEP_ARCHIVE", 1, 75},// file6
}

// buildTestIndex builds an index from the synthetic inventory and returns the path.
func buildTestIndex(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	// Aggregate using MemoryAggregator
	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	for _, obj := range syntheticInventory {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}

	// Finalize to SQLite
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	// Build index from SQLite
	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	return indexDir
}

// TestE2E_PrefixStats validates that all prefix stats are computed correctly
// through the full pipeline.
func TestE2E_PrefixStats(t *testing.T) {
	indexDir := buildTestIndex(t)

	// Open the built index
	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Validate prefix count
	expectedCount := uint64(len(expectedPrefixStats))
	if idx.Count() != expectedCount {
		t.Errorf("Count() = %d, want %d", idx.Count(), expectedCount)
	}

	// Validate each prefix's stats
	for _, exp := range expectedPrefixStats {
		t.Run("prefix="+quotePrefixForTest(exp.prefix), func(t *testing.T) {
			stats, ok := idx.StatsForPrefix(exp.prefix)
			if !ok {
				t.Fatalf("prefix %q not found in index", exp.prefix)
			}

			if stats.ObjectCount != exp.objectCount {
				t.Errorf("ObjectCount = %d, want %d", stats.ObjectCount, exp.objectCount)
			}
			if stats.TotalBytes != exp.totalBytes {
				t.Errorf("TotalBytes = %d, want %d", stats.TotalBytes, exp.totalBytes)
			}

			// Also validate depth via lookup
			pos, _ := idx.Lookup(exp.prefix)
			depth := idx.Depth(pos)
			if depth != exp.depth {
				t.Errorf("Depth = %d, want %d", depth, exp.depth)
			}
		})
	}
}

// TestE2E_TierBreakdown validates per-tier statistics.
func TestE2E_TierBreakdown(t *testing.T) {
	indexDir := buildTestIndex(t)

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	if !idx.HasTierData() {
		t.Fatal("index should have tier data")
	}

	// Group expected tier stats by prefix for validation
	byPrefix := make(map[string]map[string]expectedTierStats)
	for _, exp := range expectedTierBreakdown {
		if byPrefix[exp.prefix] == nil {
			byPrefix[exp.prefix] = make(map[string]expectedTierStats)
		}
		byPrefix[exp.prefix][exp.tierName] = exp
	}

	for prefix, tierMap := range byPrefix {
		t.Run("prefix="+quotePrefixForTest(prefix), func(t *testing.T) {
			breakdown := idx.TierBreakdownMap(lookupPos(t, idx, prefix))
			if breakdown == nil {
				t.Fatalf("no tier breakdown for prefix %q", prefix)
			}

			for tierName, exp := range tierMap {
				tb, ok := breakdown[tierName]
				if !ok {
					t.Errorf("tier %q not in breakdown", tierName)
					continue
				}
				if tb.ObjectCount != exp.count {
					t.Errorf("tier %q ObjectCount = %d, want %d", tierName, tb.ObjectCount, exp.count)
				}
				if tb.Bytes != exp.bytes {
					t.Errorf("tier %q Bytes = %d, want %d", tierName, tb.Bytes, exp.bytes)
				}
			}
		})
	}
}

// TestE2E_MaxDepth validates the maximum depth calculation.
func TestE2E_MaxDepth(t *testing.T) {
	indexDir := buildTestIndex(t)

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Our deepest prefix is "a/b/" at depth 2
	if idx.MaxDepth() != 2 {
		t.Errorf("MaxDepth() = %d, want 2", idx.MaxDepth())
	}
}

// TestE2E_SubtreeNavigation validates subtree iteration and depth-based queries.
func TestE2E_SubtreeNavigation(t *testing.T) {
	indexDir := buildTestIndex(t)

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Get root position
	rootPos, ok := idx.Lookup("")
	if !ok {
		t.Fatal("root prefix not found")
	}

	// Root's subtree should span all nodes (SubtreeEnd is the last position in subtree)
	subtreeEnd := idx.SubtreeEnd(rootPos)
	if subtreeEnd < idx.Count()-1 {
		t.Errorf("SubtreeEnd(root) = %d, expected at least %d", subtreeEnd, idx.Count()-1)
	}

	// Children of root at depth 1 should be "a/" and "c/"
	children, err := idx.DescendantsAtDepth(rootPos, 1)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("expected 2 children at depth 1, got %d", len(children))
	}

	// Verify children are "a/" and "c/"
	childPrefixes := make(map[string]bool)
	for _, pos := range children {
		prefix, err := idx.PrefixString(pos)
		if err != nil {
			t.Fatalf("PrefixString(%d) failed: %v", pos, err)
		}
		childPrefixes[prefix] = true
	}
	if !childPrefixes["a/"] {
		t.Error("missing child prefix 'a/'")
	}
	if !childPrefixes["c/"] {
		t.Error("missing child prefix 'c/'")
	}
}

// TestE2E_PrefixNotFound validates behavior for non-existent prefixes.
func TestE2E_PrefixNotFound(t *testing.T) {
	indexDir := buildTestIndex(t)

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	nonExistent := []string{
		"nonexistent/",
		"a/c/",           // valid parent, invalid child
		"file1.txt",      // object key, not a prefix
		"a/b/file3.txt",  // object key, not a prefix
	}

	for _, prefix := range nonExistent {
		t.Run("prefix="+prefix, func(t *testing.T) {
			_, ok := idx.StatsForPrefix(prefix)
			if ok {
				t.Errorf("expected prefix %q to not be found", prefix)
			}
		})
	}
}

// TestE2E_EmptyInventory validates handling of empty input.
func TestE2E_EmptyInventory(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	// Create aggregator but add no objects
	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	// Build should fail because no nodes
	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	err := BuildFromSQLite(cfg)
	if err == nil {
		t.Fatal("expected error for empty inventory, got nil")
	}
}

// TestE2E_SingleObject validates the minimal case of one object.
func TestE2E_SingleObject(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	agg.AddObject("single.txt", 1000, tiers.Standard)
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Should have exactly 1 prefix: root ""
	if idx.Count() != 1 {
		t.Errorf("Count() = %d, want 1", idx.Count())
	}

	stats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root prefix not found")
	}
	if stats.ObjectCount != 1 {
		t.Errorf("ObjectCount = %d, want 1", stats.ObjectCount)
	}
	if stats.TotalBytes != 1000 {
		t.Errorf("TotalBytes = %d, want 1000", stats.TotalBytes)
	}
}

// TestE2E_DeepHierarchy validates handling of deeply nested prefixes.
func TestE2E_DeepHierarchy(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	// Create a 10-level deep hierarchy
	agg.AddObject("a/b/c/d/e/f/g/h/i/j/file.txt", 100, tiers.Standard)
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Should have 11 prefixes: "" + 10 levels
	if idx.Count() != 11 {
		t.Errorf("Count() = %d, want 11", idx.Count())
	}

	// MaxDepth should be 10
	if idx.MaxDepth() != 10 {
		t.Errorf("MaxDepth() = %d, want 10", idx.MaxDepth())
	}

	// Verify deepest prefix
	deepest := "a/b/c/d/e/f/g/h/i/j/"
	stats, ok := idx.StatsForPrefix(deepest)
	if !ok {
		t.Fatalf("deepest prefix %q not found", deepest)
	}
	if stats.ObjectCount != 1 {
		t.Errorf("deepest ObjectCount = %d, want 1", stats.ObjectCount)
	}

	pos, _ := idx.Lookup(deepest)
	if idx.Depth(pos) != 10 {
		t.Errorf("deepest Depth = %d, want 10", idx.Depth(pos))
	}
}

// TestE2E_MultipleObjectsSamePrefix validates accumulation within a single prefix.
func TestE2E_MultipleObjectsSamePrefix(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))
	// Add 5 objects to the same prefix
	for i := 0; i < 5; i++ {
		agg.AddObject("data/file.txt", 100, tiers.Standard)
	}
	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Root should have 5 objects, 500 bytes
	stats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root prefix not found")
	}
	if stats.ObjectCount != 5 {
		t.Errorf("root ObjectCount = %d, want 5", stats.ObjectCount)
	}
	if stats.TotalBytes != 500 {
		t.Errorf("root TotalBytes = %d, want 500", stats.TotalBytes)
	}

	// data/ should also have 5 objects, 500 bytes
	stats, ok = idx.StatsForPrefix("data/")
	if !ok {
		t.Fatal("data/ prefix not found")
	}
	if stats.ObjectCount != 5 {
		t.Errorf("data/ ObjectCount = %d, want 5", stats.ObjectCount)
	}
	if stats.TotalBytes != 500 {
		t.Errorf("data/ TotalBytes = %d, want 500", stats.TotalBytes)
	}
}

// TestE2E_AllTiers validates that all storage tiers are tracked correctly.
func TestE2E_AllTiers(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	indexDir := filepath.Join(tmpDir, "index")

	agg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))

	// Add one object for each tier (using correct tier constants from tiers package)
	type tierTest struct {
		id       tiers.ID
		name     string
		size     uint64
	}
	tierTests := []tierTest{
		{tiers.Standard, "STANDARD", 100},
		{tiers.StandardIA, "STANDARD_IA", 200},
		{tiers.OneZoneIA, "ONEZONE_IA", 300},
		{tiers.GlacierIR, "GLACIER_IR", 400},
		{tiers.GlacierFR, "GLACIER", 500},
		{tiers.DeepArchive, "DEEP_ARCHIVE", 600},
		{tiers.ReducedRedundancy, "REDUCED_REDUNDANCY", 700},
		{tiers.ITFrequent, "INTELLIGENT_TIERING_FREQUENT", 800},
	}

	for _, tt := range tierTests {
		agg.AddObject("file.txt", tt.size, tt.id)
	}

	if err := agg.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	agg.Clear()

	cfg := SQLiteConfig{
		OutDir:    indexDir,
		DBPath:    dbPath,
		SQLiteCfg: sqliteagg.DefaultConfig(dbPath),
	}
	if err := BuildFromSQLite(cfg); err != nil {
		t.Fatalf("BuildFromSQLite failed: %v", err)
	}

	idx, err := indexread.Open(indexDir)
	if err != nil {
		t.Fatalf("Open index failed: %v", err)
	}
	defer idx.Close()

	// Validate total stats
	stats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root prefix not found")
	}
	expectedCount := uint64(len(tierTests))
	expectedBytes := uint64(0)
	for _, tt := range tierTests {
		expectedBytes += tt.size
	}
	if stats.ObjectCount != expectedCount {
		t.Errorf("ObjectCount = %d, want %d", stats.ObjectCount, expectedCount)
	}
	if stats.TotalBytes != expectedBytes {
		t.Errorf("TotalBytes = %d, want %d", stats.TotalBytes, expectedBytes)
	}

	// Validate tier breakdown
	breakdown := idx.TierBreakdownMap(0)
	if breakdown == nil {
		t.Fatal("no tier breakdown")
	}

	for _, tt := range tierTests {
		tb, ok := breakdown[tt.name]
		if !ok {
			t.Errorf("tier %q not in breakdown", tt.name)
			continue
		}
		if tb.ObjectCount != 1 {
			t.Errorf("tier %q ObjectCount = %d, want 1", tt.name, tb.ObjectCount)
		}
		if tb.Bytes != tt.size {
			t.Errorf("tier %q Bytes = %d, want %d", tt.name, tb.Bytes, tt.size)
		}
	}
}

// lookupPos is a helper that looks up a prefix position and fails on error.
func lookupPos(t *testing.T, idx *indexread.Index, prefix string) uint64 {
	t.Helper()
	pos, ok := idx.Lookup(prefix)
	if !ok {
		t.Fatalf("prefix %q not found", prefix)
	}
	return pos
}

// quotePrefixForTest formats a prefix for test names.
func quotePrefixForTest(prefix string) string {
	if prefix == "" {
		return "root"
	}
	return prefix
}
