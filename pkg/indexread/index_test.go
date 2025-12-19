package indexread

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
)

// createTestInventory creates a test CSV inventory file.
func createTestInventory(t *testing.T, dir, name, content string) string {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	return path
}

func TestEndToEndSimple(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Create test inventory
	csv := `Key,Size
a/file1.txt,100
a/file2.txt,200
b/file1.txt,300
b/sub/file.txt,400
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	// Build index
	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Open and query
	idx, err := Open(outDir)
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
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/x/file.txt,100
a/y/file.txt,200
a/z/file.txt,300
b/m/file.txt,400
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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
	var names1 []string
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

	var aNames []string
	for _, pos := range aDesc {
		name, _ := idx.PrefixString(pos)
		aNames = append(aNames, name)
	}
	expectedANames := []string{"a/x/", "a/y/", "a/z/"}
	if !reflect.DeepEqual(aNames, expectedANames) {
		t.Errorf("a/ descendants = %v, want %v", aNames, expectedANames)
	}
}

func TestDescendantsUpToDepth(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/b/c/file.txt,100
a/b/d/file.txt,200
a/e/file.txt,300
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	// Get descendants up to depth 3
	grouped, err := idx.DescendantsUpToDepth(rootPos, 3)
	if err != nil {
		t.Fatalf("DescendantsUpToDepth failed: %v", err)
	}

	// Should have 3 groups (depth 1, 2, 3)
	if len(grouped) != 3 {
		t.Errorf("got %d depth groups, want 3", len(grouped))
	}

	// Depth 1: a/
	if len(grouped) > 0 && len(grouped[0]) != 1 {
		t.Errorf("depth 1 has %d entries, want 1", len(grouped[0]))
	}

	// Depth 2: a/b/, a/e/
	if len(grouped) > 1 && len(grouped[1]) != 2 {
		t.Errorf("depth 2 has %d entries, want 2", len(grouped[1]))
	}

	// Depth 3: a/b/c/, a/b/d/
	if len(grouped) > 2 && len(grouped[2]) != 2 {
		t.Errorf("depth 3 has %d entries, want 2", len(grouped[2]))
	}
}

func TestFiltering(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
small/file.txt,10
medium/file1.txt,100
medium/file2.txt,100
large/file1.txt,500
large/file2.txt,500
large/file3.txt,500
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	// Filter by min count >= 2
	filtered, err := idx.DescendantsAtDepthFiltered(rootPos, 1, Filter{MinCount: 2})
	if err != nil {
		t.Fatalf("DescendantsAtDepthFiltered failed: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("got %d filtered results, want 2 (medium/, large/)", len(filtered))
	}

	// Filter by min bytes >= 500
	filtered2, err := idx.DescendantsAtDepthFiltered(rootPos, 1, Filter{MinBytes: 500})
	if err != nil {
		t.Fatalf("DescendantsAtDepthFiltered failed: %v", err)
	}
	if len(filtered2) != 1 {
		t.Errorf("got %d filtered results, want 1 (large/)", len(filtered2))
	}
}

func TestIterator(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/file.txt,100
b/file.txt,200
c/file.txt,300
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	it, err := idx.NewDescendantIterator(rootPos, 1)
	if err != nil {
		t.Fatalf("NewDescendantIterator failed: %v", err)
	}

	var positions []uint64
	for it.Next() {
		positions = append(positions, it.Pos())
	}

	if len(positions) != 3 {
		t.Errorf("iterator returned %d positions, want 3", len(positions))
	}
}

func TestMultipleInventoryFiles(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv1 := `Key,Size
a/file1.txt,100
c/file1.txt,300
`
	csv2 := `Key,Size
b/file1.txt,200
d/file1.txt,400
`
	invPath1 := createTestInventory(t, tmpDir, "inv1.csv", csv1)
	invPath2 := createTestInventory(t, tmpDir, "inv2.csv", csv2)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath1, invPath2}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// All 4 top-level prefixes should exist
	for _, prefix := range []string{"a/", "b/", "c/", "d/"} {
		_, ok := idx.StatsForPrefix(prefix)
		if !ok {
			t.Errorf("prefix %q not found", prefix)
		}
	}

	rootStats, _ := idx.StatsForPrefix("")
	if rootStats.ObjectCount != 4 {
		t.Errorf("root count = %d, want 4", rootStats.ObjectCount)
	}
	if rootStats.TotalBytes != 1000 {
		t.Errorf("root bytes = %d, want 1000", rootStats.TotalBytes)
	}
}

func TestPrefixStringRetrieval(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
foo/bar/file.txt,100
foo/baz/file.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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

	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Generate large inventory
	var csvContent string
	csvContent = "Key,Size\n"
	expectedPrefixes := make(map[string]uint64)
	expectedPrefixes[""] = 0

	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			key := prefixFromInt(i) + prefixFromInt(j) + "file.txt"
			size := uint64(i*10 + j)
			csvContent += key + "," + string('0'+byte(size%10)) + "\n"

			// Track expected prefixes
			p1 := prefixFromInt(i)
			p2 := prefixFromInt(i) + prefixFromInt(j)
			expectedPrefixes[p1]++
			expectedPrefixes[p2]++
			expectedPrefixes[""]++
		}
	}

	invPath := createTestInventory(t, tmpDir, "large.csv", csvContent)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 200,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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

// Helper to generate unique prefix strings
func prefixFromInt(i int) string {
	return string('a'+byte(i%26)) + string('a'+byte(i/26%26)) + "/"
}

func TestManifestCreatedAndVerifiable(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/file.txt,100
b/file.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
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

	// Verify all files have checksums
	expectedFiles := []string{
		"subtree_end.u64",
		"depth.u32",
		"object_count.u64",
		"total_bytes.u64",
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
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/b/c/file.txt,100
x/y/file.txt,200
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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

func TestEmptyIterator(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/file.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Query for descendants at depth 5 (doesn't exist)
	rootPos, _ := idx.Lookup("")
	it, err := idx.NewDescendantIterator(rootPos, 5)
	if err != nil {
		t.Fatalf("NewDescendantIterator failed: %v", err)
	}

	// Should return no results
	count := 0
	for it.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Expected 0 results at depth 5, got %d", count)
	}

	// Test iterator on invalid position
	it2, err := idx.NewDescendantIterator(999999, 1)
	if err != nil {
		t.Fatalf("NewDescendantIterator failed: %v", err)
	}
	if it2.Next() {
		t.Error("Expected empty iterator for invalid position")
	}
}

func TestIteratorDepthMethod(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	csv := `Key,Size
a/b/file.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")
	it, err := idx.NewDescendantIterator(rootPos, 2)
	if err != nil {
		t.Fatalf("NewDescendantIterator failed: %v", err)
	}

	if it.Next() {
		// Depth should be 2 (root depth 0 + relative depth 2)
		if it.Depth() != 2 {
			t.Errorf("Depth() = %d, want 2", it.Depth())
		}
	}
}

func TestSingleNodeTrie(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Single file at root level (no prefix directories)
	csv := `Key,Size
file.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Very deep path
	csv := `Key,Size
a/b/c/d/e/f/g/h/i/j/file.txt,100
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
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
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	os.MkdirAll(sortDir, 0755)

	// Create inventory with specific ordering
	csv := `Key,Size
zebra/file.txt,100
apple/file.txt,200
mango/file.txt,300
banana/file.txt,400
`
	invPath := createTestInventory(t, tmpDir, "test.csv", csv)

	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")
	desc, err := idx.DescendantsAtDepth(rootPos, 1)
	if err != nil {
		t.Fatalf("DescendantsAtDepth failed: %v", err)
	}

	var names []string
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
