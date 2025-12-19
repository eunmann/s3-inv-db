package triebuild

import (
	"reflect"
	"sort"
	"testing"
)

func TestExtractPrefixes(t *testing.T) {
	tests := []struct {
		key      string
		expected []string
	}{
		{"file.txt", nil},
		{"a/file.txt", []string{"a/"}},
		{"a/b/file.txt", []string{"a/", "a/b/"}},
		{"a/b/c/file.txt", []string{"a/", "a/b/", "a/b/c/"}},
		{"a/", []string{"a/"}},
		{"a/b/", []string{"a/", "a/b/"}},
		{"a//b.txt", []string{"a/", "a//"}},
	}

	for _, tc := range tests {
		result := extractPrefixes(tc.key)
		if !reflect.DeepEqual(result, tc.expected) {
			t.Errorf("extractPrefixes(%q) = %v, want %v", tc.key, result, tc.expected)
		}
	}
}

func TestBuildFromKeys_Simple(t *testing.T) {
	keys := []string{
		"a/file1.txt",
		"a/file2.txt",
		"b/file1.txt",
	}
	sizes := []uint64{100, 200, 300}

	result, err := BuildFromKeys(keys, sizes)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Should have: root "", "a/", "b/"
	if len(result.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(result.Nodes))
	}

	// Verify root
	root := result.Nodes[0]
	if root.Prefix != "" {
		t.Errorf("root prefix = %q, want empty", root.Prefix)
	}
	if root.Depth != 0 {
		t.Errorf("root depth = %d, want 0", root.Depth)
	}
	if root.ObjectCount != 3 {
		t.Errorf("root count = %d, want 3", root.ObjectCount)
	}
	if root.TotalBytes != 600 {
		t.Errorf("root bytes = %d, want 600", root.TotalBytes)
	}

	// Verify a/
	nodeA, ok := result.GetNodeByPrefix("a/")
	if !ok {
		t.Fatal("node a/ not found")
	}
	if nodeA.ObjectCount != 2 {
		t.Errorf("a/ count = %d, want 2", nodeA.ObjectCount)
	}
	if nodeA.TotalBytes != 300 {
		t.Errorf("a/ bytes = %d, want 300", nodeA.TotalBytes)
	}

	// Verify b/
	nodeB, ok := result.GetNodeByPrefix("b/")
	if !ok {
		t.Fatal("node b/ not found")
	}
	if nodeB.ObjectCount != 1 {
		t.Errorf("b/ count = %d, want 1", nodeB.ObjectCount)
	}
}

func TestBuildFromKeys_Nested(t *testing.T) {
	keys := []string{
		"a/b/c/file.txt",
		"a/b/d/file.txt",
		"a/e/file.txt",
	}
	sizes := []uint64{100, 200, 300}

	result, err := BuildFromKeys(keys, sizes)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Should have: "", "a/", "a/b/", "a/b/c/", "a/b/d/", "a/e/"
	expectedPrefixes := []string{"", "a/", "a/b/", "a/b/c/", "a/b/d/", "a/e/"}
	actualPrefixes := result.PrefixStrings()
	sort.Strings(actualPrefixes)
	sort.Strings(expectedPrefixes)

	if !reflect.DeepEqual(actualPrefixes, expectedPrefixes) {
		t.Errorf("prefixes = %v, want %v", actualPrefixes, expectedPrefixes)
	}

	// Verify depths
	for _, n := range result.Nodes {
		expectedDepth := uint32(countSlashes(n.Prefix))
		if n.Depth != expectedDepth {
			t.Errorf("depth of %q = %d, want %d", n.Prefix, n.Depth, expectedDepth)
		}
	}

	// Verify a/b/ stats
	nodeAB, ok := result.GetNodeByPrefix("a/b/")
	if !ok {
		t.Fatal("node a/b/ not found")
	}
	if nodeAB.ObjectCount != 2 {
		t.Errorf("a/b/ count = %d, want 2", nodeAB.ObjectCount)
	}
	if nodeAB.TotalBytes != 300 {
		t.Errorf("a/b/ bytes = %d, want 300", nodeAB.TotalBytes)
	}
}

func TestBuildFromKeys_SubtreeRanges(t *testing.T) {
	keys := []string{
		"a/x/file.txt",
		"a/y/file.txt",
		"b/z/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	if !result.VerifySubtreeRanges() {
		t.Error("subtree ranges verification failed")
	}

	// Root should span all nodes
	root := result.Nodes[0]
	if root.SubtreeEnd != uint64(len(result.Nodes)-1) {
		t.Errorf("root subtree_end = %d, want %d", root.SubtreeEnd, len(result.Nodes)-1)
	}

	// Find a/ and verify its subtree
	nodeA, ok := result.GetNodeByPrefix("a/")
	if !ok {
		t.Fatal("node a/ not found")
	}

	descendants := result.GetDescendants(nodeA.Pos)
	// a/ should have descendants: a/x/, a/y/
	if len(descendants) != 2 {
		t.Errorf("a/ has %d descendants, want 2", len(descendants))
	}

	for _, d := range descendants {
		if d.Prefix != "a/x/" && d.Prefix != "a/y/" {
			t.Errorf("unexpected descendant of a/: %q", d.Prefix)
		}
	}
}

func TestBuildFromKeys_SingleLevel(t *testing.T) {
	keys := []string{
		"file1.txt",
		"file2.txt",
		"file3.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Only root node
	if len(result.Nodes) != 1 {
		t.Errorf("got %d nodes, want 1", len(result.Nodes))
	}

	root := result.Nodes[0]
	if root.ObjectCount != 3 {
		t.Errorf("root count = %d, want 3", root.ObjectCount)
	}
	if root.MaxDepthInSubtree != 0 {
		t.Errorf("root maxDepth = %d, want 0", root.MaxDepthInSubtree)
	}
}

func TestBuildFromKeys_FolderMarkers(t *testing.T) {
	keys := []string{
		"folder/",
		"folder/file.txt",
		"folder/sub/",
		"folder/sub/file.txt",
	}
	sizes := []uint64{0, 100, 0, 200}

	result, err := BuildFromKeys(keys, sizes)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// folder/ and folder/sub/ should exist
	folderNode, ok := result.GetNodeByPrefix("folder/")
	if !ok {
		t.Fatal("folder/ not found")
	}
	if folderNode.ObjectCount != 4 {
		t.Errorf("folder/ count = %d, want 4", folderNode.ObjectCount)
	}

	subNode, ok := result.GetNodeByPrefix("folder/sub/")
	if !ok {
		t.Fatal("folder/sub/ not found")
	}
	if subNode.ObjectCount != 2 {
		t.Errorf("folder/sub/ count = %d, want 2", subNode.ObjectCount)
	}
}

func TestBuildFromKeys_Empty(t *testing.T) {
	result, err := BuildFromKeys(nil, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Should have just root node
	if len(result.Nodes) != 1 {
		t.Errorf("got %d nodes, want 1", len(result.Nodes))
	}

	root := result.Nodes[0]
	if root.ObjectCount != 0 {
		t.Errorf("root count = %d, want 0", root.ObjectCount)
	}
}

func TestBuildFromKeys_MaxDepth(t *testing.T) {
	keys := []string{
		"a/b/c/d/e/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	if result.MaxDepth != 5 {
		t.Errorf("maxDepth = %d, want 5", result.MaxDepth)
	}

	// Root should track max depth in subtree
	root := result.Nodes[0]
	if root.MaxDepthInSubtree != 5 {
		t.Errorf("root maxDepthInSubtree = %d, want 5", root.MaxDepthInSubtree)
	}
}

func TestBuildFromKeys_Unicode(t *testing.T) {
	keys := []string{
		"日本語/ファイル.txt",
		"日本語/別のファイル.txt",
		"한국어/파일.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	jpNode, ok := result.GetNodeByPrefix("日本語/")
	if !ok {
		t.Fatal("日本語/ not found")
	}
	if jpNode.ObjectCount != 2 {
		t.Errorf("日本語/ count = %d, want 2", jpNode.ObjectCount)
	}

	krNode, ok := result.GetNodeByPrefix("한국어/")
	if !ok {
		t.Fatal("한국어/ not found")
	}
	if krNode.ObjectCount != 1 {
		t.Errorf("한국어/ count = %d, want 1", krNode.ObjectCount)
	}
}

func TestBuildFromKeys_RepeatedKeys(t *testing.T) {
	keys := []string{
		"a/file.txt",
		"a/file.txt",
		"a/file.txt",
	}
	sizes := []uint64{100, 100, 100}

	result, err := BuildFromKeys(keys, sizes)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	nodeA, ok := result.GetNodeByPrefix("a/")
	if !ok {
		t.Fatal("a/ not found")
	}
	if nodeA.ObjectCount != 3 {
		t.Errorf("a/ count = %d, want 3", nodeA.ObjectCount)
	}
	if nodeA.TotalBytes != 300 {
		t.Errorf("a/ bytes = %d, want 300", nodeA.TotalBytes)
	}
}

func TestBuildFromKeys_ConsecutiveSlashes(t *testing.T) {
	keys := []string{
		"a//b/file.txt",
		"a//c/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Should have: "", "a/", "a//", "a//b/", "a//c/"
	expectedPrefixes := []string{"", "a/", "a//", "a//b/", "a//c/"}
	actualPrefixes := result.PrefixStrings()
	sort.Strings(actualPrefixes)
	sort.Strings(expectedPrefixes)

	if !reflect.DeepEqual(actualPrefixes, expectedPrefixes) {
		t.Errorf("prefixes = %v, want %v", actualPrefixes, expectedPrefixes)
	}
}

func TestBuildFromKeys_LongPath(t *testing.T) {
	// Create a very deep path
	var keys []string
	path := ""
	for i := 0; i < 50; i++ {
		path += "dir/"
	}
	path += "file.txt"
	keys = append(keys, path)

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	if result.MaxDepth != 50 {
		t.Errorf("maxDepth = %d, want 50", result.MaxDepth)
	}

	// Should have 51 nodes (root + 50 levels)
	if len(result.Nodes) != 51 {
		t.Errorf("got %d nodes, want 51", len(result.Nodes))
	}
}

func TestBuildFromKeys_PreorderPositions(t *testing.T) {
	keys := []string{
		"a/x/file.txt",
		"a/y/file.txt",
		"b/z/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Verify all positions match indices
	for i, n := range result.Nodes {
		if n.Pos != uint64(i) {
			t.Errorf("node %d has pos %d", i, n.Pos)
		}
	}

	// Verify preorder: root comes first, then a/, a/x/, a/y/, then b/, b/z/
	// Due to closing order, the actual preorder is:
	// 0: root, 1: a/, 2: a/x/, 3: a/y/, 4: b/, 5: b/z/
	expectedOrder := []string{"", "a/", "a/x/", "a/y/", "b/", "b/z/"}
	for i, expected := range expectedOrder {
		if result.Nodes[i].Prefix != expected {
			t.Errorf("pos %d has prefix %q, want %q", i, result.Nodes[i].Prefix, expected)
		}
	}
}

func TestResult_VerifySubtreeRanges(t *testing.T) {
	keys := []string{
		"a/b/file.txt",
		"a/c/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	if !result.VerifySubtreeRanges() {
		t.Error("VerifySubtreeRanges failed")
	}
	if !result.VerifyDepthOrder() {
		t.Error("VerifyDepthOrder failed")
	}
}

func TestResult_GetDescendants(t *testing.T) {
	keys := []string{
		"a/b/file.txt",
		"a/c/file.txt",
		"d/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Root descendants should be all other nodes
	rootDesc := result.GetDescendants(0)
	if len(rootDesc) != len(result.Nodes)-1 {
		t.Errorf("root has %d descendants, want %d", len(rootDesc), len(result.Nodes)-1)
	}

	// Out of bounds
	outDesc := result.GetDescendants(100)
	if outDesc != nil {
		t.Errorf("out of bounds should return nil, got %v", outDesc)
	}
}

func TestBuildFromKeys_AlphabeticalOrder(t *testing.T) {
	// Keys should be sorted lexicographically for correct trie build
	keys := []string{
		"a/file.txt",
		"b/file.txt",
		"c/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Nodes within same depth should be in alphabetical order by prefix
	// Check that a/ comes before b/ comes before c/
	aPos := uint64(0)
	bPos := uint64(0)
	cPos := uint64(0)

	for _, n := range result.Nodes {
		switch n.Prefix {
		case "a/":
			aPos = n.Pos
		case "b/":
			bPos = n.Pos
		case "c/":
			cPos = n.Pos
		}
	}

	if aPos > bPos || bPos > cPos {
		t.Errorf("nodes not in alphabetical order: a=%d, b=%d, c=%d", aPos, bPos, cPos)
	}
}

// Helper function
func countSlashes(s string) int {
	count := 0
	for _, c := range s {
		if c == '/' {
			count++
		}
	}
	return count
}

func TestBuildFromKeys_MixedDepths(t *testing.T) {
	keys := []string{
		"file.txt",
		"a/file.txt",
		"a/b/file.txt",
		"c/file.txt",
	}

	result, err := BuildFromKeys(keys, nil)
	if err != nil {
		t.Fatalf("BuildFromKeys failed: %v", err)
	}

	// Root should count all objects
	root := result.Nodes[0]
	if root.ObjectCount != 4 {
		t.Errorf("root count = %d, want 4", root.ObjectCount)
	}

	// a/ should count 2 objects
	nodeA, ok := result.GetNodeByPrefix("a/")
	if !ok {
		t.Fatal("a/ not found")
	}
	if nodeA.ObjectCount != 2 {
		t.Errorf("a/ count = %d, want 2", nodeA.ObjectCount)
	}

	// a/b/ should count 1 object
	nodeAB, ok := result.GetNodeByPrefix("a/b/")
	if !ok {
		t.Fatal("a/b/ not found")
	}
	if nodeAB.ObjectCount != 1 {
		t.Errorf("a/b/ count = %d, want 1", nodeAB.ObjectCount)
	}
}
