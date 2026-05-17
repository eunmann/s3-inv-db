package indexread_test

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// minimalCSV is a one-row inventory used by tests that just need a valid
// index to open; extracted to a constant to satisfy goconst.
const minimalCSV = `Key,Size
a/file.txt,100
`

func TestEndToEndSimple(t *testing.T) {
	outDir := setupTestIndex(t)

	// Create test inventory
	csv := `Key,Size
a/file1.txt,100
a/file2.txt,200
b/file1.txt,300
b/sub/file.txt,400
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Open and query
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Test root stats
	rootStats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root lookup failed")
	}
	if rootStats.ObjectCount != 4 {
		t.Errorf("root count = %d, want 4", rootStats.ObjectCount)
	}
	if rootStats.TotalBytes != 1000 {
		t.Errorf("root bytes = %d, want 1000", rootStats.TotalBytes)
	}

	// Test a/ stats
	aStats, ok := idx.StatsForPrefix("a/")
	if !ok {
		t.Fatal("a/ lookup failed")
	}
	if aStats.ObjectCount != 2 {
		t.Errorf("a/ count = %d, want 2", aStats.ObjectCount)
	}
	if aStats.TotalBytes != 300 {
		t.Errorf("a/ bytes = %d, want 300", aStats.TotalBytes)
	}

	// Test b/ stats
	bStats, ok := idx.StatsForPrefix("b/")
	if !ok {
		t.Fatal("b/ lookup failed")
	}
	if bStats.ObjectCount != 2 {
		t.Errorf("b/ count = %d, want 2", bStats.ObjectCount)
	}
	if bStats.TotalBytes != 700 {
		t.Errorf("b/ bytes = %d, want 700", bStats.TotalBytes)
	}

	// Test b/sub/ stats
	subStats, ok := idx.StatsForPrefix("b/sub/")
	if !ok {
		t.Fatal("b/sub/ lookup failed")
	}
	if subStats.ObjectCount != 1 {
		t.Errorf("b/sub/ count = %d, want 1", subStats.ObjectCount)
	}

	// Test non-existent prefix
	_, ok = idx.StatsForPrefix("nonexistent/")
	if ok {
		t.Error("nonexistent/ should not be found")
	}
}

func TestDescendantsAtDepth(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/x/file.txt,100
a/y/file.txt,200
a/z/file.txt,300
b/m/file.txt,400
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Get root position
	rootPos, ok := idx.Lookup("")
	if !ok {
		t.Fatal("root lookup failed")
	}

	// Descendants at depth 1 (a/, b/)
	desc1, err := idx.DescendantsAtDepth(rootPos, 1)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}
	if len(desc1) != 2 {
		t.Errorf("got %d descendants at depth 1, want 2", len(desc1))
	}

	// Verify alphabetical order
	names1 := make([]string, 0, len(desc1))
	for _, pos := range desc1 {
		name, _ := idx.PrefixString(pos)
		names1 = append(names1, name)
	}
	expectedNames1 := []string{"a/", "b/"}
	if !reflect.DeepEqual(names1, expectedNames1) {
		t.Errorf("depth 1 names = %v, want %v", names1, expectedNames1)
	}

	// Descendants at depth 2 (a/x/, a/y/, a/z/, b/m/)
	desc2, err := idx.DescendantsAtDepth(rootPos, 2)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}
	if len(desc2) != 4 {
		t.Errorf("got %d descendants at depth 2, want 4", len(desc2))
	}

	// Get a/ position and query its descendants
	aPos, ok := idx.Lookup("a/")
	if !ok {
		t.Fatal("a/ lookup failed")
	}

	aDesc, err := idx.DescendantsAtDepth(aPos, 1)
	if err != nil {
		t.Fatalf("DescendantsAtDepth for a/ failed: %v", err)
	}
	if len(aDesc) != 3 {
		t.Errorf("a/ has %d descendants at depth 1, want 3", len(aDesc))
	}

	aNames := make([]string, 0, len(aDesc))
	for _, pos := range aDesc {
		name, _ := idx.PrefixString(pos)
		aNames = append(aNames, name)
	}
	expectedANames := []string{"a/x/", "a/y/", "a/z/"}
	if !reflect.DeepEqual(aNames, expectedANames) {
		t.Errorf("a/ descendants = %v, want %v", aNames, expectedANames)
	}
}

func TestFiltering(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
small/file.txt,10
medium/file1.txt,100
medium/file2.txt,100
large/file1.txt,500
large/file2.txt,500
large/file3.txt,500
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	// Filter by min count >= 2
	filtered, err := idx.DescendantsAtDepthFiltered(rootPos, 1, indexread.Filter{MinCount: 2})
	if err != nil {
		t.Fatalf("DescendantsAtDepthFiltered failed: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("got %d filtered results, want 2 (medium/, large/)", len(filtered))
	}

	// Filter by min bytes >= 500
	filtered2, err := idx.DescendantsAtDepthFiltered(rootPos, 1, indexread.Filter{MinBytes: 500})
	if err != nil {
		t.Fatalf("DescendantsAtDepthFiltered failed: %v", err)
	}
	if len(filtered2) != 1 {
		t.Errorf("got %d filtered results, want 1 (large/)", len(filtered2))
	}
}

func TestPrefixStringRetrieval(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
foo/bar/file.txt,100
foo/baz/file.txt,200
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// For each known prefix, lookup and retrieve
	prefixes := []string{"", "foo/", "foo/bar/", "foo/baz/"}
	for _, prefix := range prefixes {
		pos, ok := idx.Lookup(prefix)
		if !ok {
			t.Errorf("Lookup(%q) failed", prefix)

			continue
		}

		retrieved, err := idx.PrefixString(pos)
		if err != nil {
			t.Errorf("PrefixString(%d) failed: %v", pos, err)

			continue
		}

		if retrieved != prefix {
			t.Errorf("PrefixString(%d) = %q, want %q", pos, retrieved, prefix)
		}
	}
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	outDir := setupTestIndex(t)

	// Generate keys
	keys := make([]string, 0, 100*10)
	for i := range 100 {
		for j := range 10 {
			key := prefixFromInt(i) + prefixFromInt(j) + "file.txt"
			keys = append(keys, key)
		}
	}

	if err := buildIndexFromKeys(t, outDir, keys); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Sample check
	rootStats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root lookup failed")
	}
	if rootStats.ObjectCount != 1000 {
		t.Errorf("root count = %d, want 1000", rootStats.ObjectCount)
	}
}

// Helper to generate unique prefix strings.
func prefixFromInt(i int) string {
	return string('a'+byte(i%26)) + string('a'+byte(i/26%26)) + "/"
}

func TestManifestCreatedAndVerifiable(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/file.txt,100
b/file.txt,200
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Check manifest exists
	manifestPath := filepath.Join(outDir, "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest.json not found: %v", err)
	}

	// Read manifest
	manifest, err := format.ReadManifest(outDir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	// Verify manifest metadata
	if manifest.Version != format.ManifestVersion {
		t.Errorf("Version = %d, want %d", manifest.Version, format.ManifestVersion)
	}
	if manifest.NodeCount != 3 { // root, a/, b/
		t.Errorf("NodeCount = %d, want 3", manifest.NodeCount)
	}
	if manifest.MaxDepth != 1 {
		t.Errorf("MaxDepth = %d, want 1", manifest.MaxDepth)
	}

	// Verify the expected file set has checksums. Indexes built with
	// the row-major layout (Q2) replace the five per-column files
	// with core_stats.bin; older indexes carry the per-column files.
	// One of the two file sets must be present.
	expectedFiles := []string{
		format.CoreStatsFile,
		"mph.bin",
	}
	for _, name := range expectedFiles {
		if _, ok := manifest.Files[name]; !ok {
			t.Errorf("File %q not in manifest", name)
		}
	}

	// Verify manifest (checksums match)
	if err := format.VerifyManifest(outDir, manifest); err != nil {
		t.Errorf("VerifyManifest failed: %v", err)
	}
}

func TestIndexCountAndMaxDepth(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/b/c/file.txt,100
x/y/file.txt,200
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Count should be 6: "", "a/", "a/b/", "a/b/c/", "x/", "x/y/"
	if idx.Count() != 6 {
		t.Errorf("Count() = %d, want 6", idx.Count())
	}

	// MaxDepth should be 3 (a/b/c/)
	if idx.MaxDepth() != 3 {
		t.Errorf("MaxDepth() = %d, want 3", idx.MaxDepth())
	}
}

func TestSingleNodeTrie(t *testing.T) {
	outDir := setupTestIndex(t)

	// Single file at root level (no prefix directories)
	csv := `Key,Size
file.txt,100
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Only root node should exist
	if idx.Count() != 1 {
		t.Errorf("Count() = %d, want 1", idx.Count())
	}

	stats, ok := idx.StatsForPrefix("")
	if !ok {
		t.Fatal("root lookup failed")
	}
	if stats.ObjectCount != 1 {
		t.Errorf("root count = %d, want 1", stats.ObjectCount)
	}
	if stats.TotalBytes != 100 {
		t.Errorf("root bytes = %d, want 100", stats.TotalBytes)
	}
}

func TestDeepPaths(t *testing.T) {
	outDir := setupTestIndex(t)

	// Very deep path
	csv := `Key,Size
a/b/c/d/e/f/g/h/i/j/file.txt,100
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// MaxDepth should be 10
	if idx.MaxDepth() != 10 {
		t.Errorf("MaxDepth() = %d, want 10", idx.MaxDepth())
	}

	// Check deepest prefix exists
	stats, ok := idx.StatsForPrefix("a/b/c/d/e/f/g/h/i/j/")
	if !ok {
		t.Fatal("deep prefix lookup failed")
	}
	if stats.ObjectCount != 1 {
		t.Errorf("deep prefix count = %d, want 1", stats.ObjectCount)
	}

	// Query descendants at depth 5 from root
	rootPos, _ := idx.Lookup("")
	desc, err := idx.DescendantsAtDepth(rootPos, 5)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}
	if len(desc) != 1 {
		t.Errorf("got %d descendants at depth 5, want 1", len(desc))
	}
}

func TestAlphabeticalOrdering(t *testing.T) {
	outDir := setupTestIndex(t)

	// Create inventory with specific ordering
	csv := `Key,Size
zebra/file.txt,100
apple/file.txt,200
mango/file.txt,300
banana/file.txt,400
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")
	desc, err := idx.DescendantsAtDepth(rootPos, 1)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}

	names := make([]string, 0, len(desc))
	for _, pos := range desc {
		name, _ := idx.PrefixString(pos)
		names = append(names, name)
	}

	expected := []string{"apple/", "banana/", "mango/", "zebra/"}
	if !reflect.DeepEqual(names, expected) {
		t.Errorf("names = %v, want %v", names, expected)
	}

	// Verify sorted
	sortedNames := make([]string, len(names))
	copy(sortedNames, names)
	sort.Strings(sortedNames)
	if !reflect.DeepEqual(names, sortedNames) {
		t.Error("names not in alphabetical order")
	}
}

func TestTierBreakdown(t *testing.T) {
	outDir := setupTestIndex(t)

	// Create objects with different storage tiers
	objects := []testObject{
		{"standard/file1.txt", 100, tiers.Standard},
		{"standard/file2.txt", 200, tiers.Standard},
		{"glacier/file1.txt", 500, tiers.GlacierFR},
		{"glacier/file2.txt", 500, tiers.GlacierFR},
		{"mixed/standard.txt", 100, tiers.Standard},
		{"mixed/glacier.txt", 400, tiers.GlacierFR},
		{"mixed/deep.txt", 200, tiers.DeepArchive},
	}

	if err := buildIndexWithTiers(t, outDir, objects); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Check tier data is available
	if !idx.HasTierData() {
		t.Fatal("HasTierData() returned false, expected true")
	}

	// Test root tier breakdown - should have all tiers
	rootBreakdown := idx.TierBreakdownForPrefix("")
	if len(rootBreakdown) == 0 {
		t.Fatal("root tier breakdown is empty")
	}

	// Find Standard tier in root breakdown
	var standardFound bool
	var standardBytes, standardCount uint64
	for _, tb := range rootBreakdown {
		if tb.TierID == tiers.Standard {
			standardFound = true
			standardBytes = tb.Bytes
			standardCount = tb.ObjectCount
		}
	}
	if !standardFound {
		t.Error("Standard tier not found in root breakdown")
	}
	if standardCount != 3 {
		t.Errorf("Standard count = %d, want 3", standardCount)
	}
	if standardBytes != 400 { // 100 + 200 + 100
		t.Errorf("Standard bytes = %d, want 400", standardBytes)
	}

	// Test prefix-specific breakdown
	glacierBreakdown := idx.TierBreakdownForPrefix("glacier/")
	if len(glacierBreakdown) == 0 {
		t.Fatal("glacier/ tier breakdown is empty")
	}

	// All should be Glacier Flexible Retrieval
	for _, tb := range glacierBreakdown {
		if tb.TierID != tiers.GlacierFR {
			continue
		}
		if tb.ObjectCount != 2 {
			t.Errorf("glacier/ GlacierFR count = %d, want 2", tb.ObjectCount)
		}
		if tb.Bytes != 1000 {
			t.Errorf("glacier/ GlacierFR bytes = %d, want 1000", tb.Bytes)
		}
	}

	// Test mixed prefix breakdown
	mixedBreakdown := idx.TierBreakdownForPrefix("mixed/")
	if len(mixedBreakdown) < 3 {
		t.Errorf("mixed/ tier breakdown has %d entries, want at least 3", len(mixedBreakdown))
	}
}

func TestTierBreakdownMap(t *testing.T) {
	outDir := setupTestIndex(t)

	objects := []testObject{
		{"a/standard.txt", 100, tiers.Standard},
		{"a/ia.txt", 200, tiers.StandardIA},
		{"a/glacier.txt", 300, tiers.GlacierFR},
	}

	if err := buildIndexWithTiers(t, outDir, objects); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Get breakdown as map
	aPos, ok := idx.Lookup("a/")
	if !ok {
		t.Fatal("a/ lookup failed")
	}

	breakdown := idx.TierBreakdownMap(aPos)
	if len(breakdown) != 3 {
		t.Errorf("breakdown map has %d entries, want 3", len(breakdown))
	}

	// Check Standard tier
	if std, ok := breakdown["STANDARD"]; ok {
		if std.ObjectCount != 1 {
			t.Errorf("Standard count = %d, want 1", std.ObjectCount)
		}
		if std.Bytes != 100 {
			t.Errorf("Standard bytes = %d, want 100", std.Bytes)
		}
	} else {
		t.Error("STANDARD tier not found in breakdown map")
	}
}

func TestNoTierData(t *testing.T) {
	outDir := setupTestIndex(t)

	// Build index without using buildIndexWithTiers (uses Standard tier only)
	if err := buildIndexFromCSV(t, outDir, minimalCSV); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// When only one tier (Standard) is used, tier tracking may be disabled
	// This is implementation-dependent, so we just verify no panic
	breakdown := idx.TierBreakdownForPrefix("")
	// With a single tier, breakdown may be empty or contain just Standard
	_ = breakdown
}
