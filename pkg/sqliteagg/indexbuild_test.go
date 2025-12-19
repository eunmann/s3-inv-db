package sqliteagg

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestBuildTrieFromSQLite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add objects to create a hierarchy:
	// ""
	// ├── a/
	// │   ├── b/
	// │   │   └── file3.txt (300 bytes)
	// │   ├── file1.txt (100 bytes)
	// │   └── file2.txt (200 bytes)
	// └── c/
	//     └── file4.txt (400 bytes)

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	testObjects := []struct {
		key    string
		size   uint64
		tierID tiers.ID
	}{
		{"a/file1.txt", 100, tiers.Standard},
		{"a/file2.txt", 200, tiers.Standard},
		{"a/b/file3.txt", 300, tiers.GlacierFR},
		{"c/file4.txt", 400, tiers.DeepArchive},
	}

	for _, obj := range testObjects {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s) failed: %v", obj.key, err)
		}
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Build trie from SQLite
	result, err := BuildTrieFromSQLite(agg)
	if err != nil {
		t.Fatalf("BuildTrieFromSQLite failed: %v", err)
	}

	// Verify node count
	// Expected prefixes: "", "a/", "a/b/", "c/"
	if len(result.Nodes) != 4 {
		t.Errorf("len(Nodes) = %d, want 4", len(result.Nodes))
	}

	// Verify max depth (a/b/ is depth 2)
	if result.MaxDepth != 2 {
		t.Errorf("MaxDepth = %d, want 2", result.MaxDepth)
	}

	// Verify root node
	root := result.Nodes[0]
	if root.Prefix != "" {
		t.Errorf("root.Prefix = %q, want \"\"", root.Prefix)
	}
	if root.ObjectCount != 4 {
		t.Errorf("root.ObjectCount = %d, want 4", root.ObjectCount)
	}
	if root.TotalBytes != 1000 {
		t.Errorf("root.TotalBytes = %d, want 1000", root.TotalBytes)
	}
	if root.SubtreeEnd != 3 {
		t.Errorf("root.SubtreeEnd = %d, want 3", root.SubtreeEnd)
	}
	if root.MaxDepthInSubtree != 2 {
		t.Errorf("root.MaxDepthInSubtree = %d, want 2", root.MaxDepthInSubtree)
	}

	// Find a/ node (should be at pos 1)
	var aNode *struct {
		found bool
		idx   int
	}
	for i, n := range result.Nodes {
		if n.Prefix == "a/" {
			aNode = &struct {
				found bool
				idx   int
			}{true, i}
			break
		}
	}
	if aNode == nil || !aNode.found {
		t.Fatal("a/ node not found")
	}

	nodeA := result.Nodes[aNode.idx]
	if nodeA.ObjectCount != 3 {
		t.Errorf("a/ ObjectCount = %d, want 3", nodeA.ObjectCount)
	}
	if nodeA.TotalBytes != 600 {
		t.Errorf("a/ TotalBytes = %d, want 600", nodeA.TotalBytes)
	}
	if nodeA.Depth != 1 {
		t.Errorf("a/ Depth = %d, want 1", nodeA.Depth)
	}
	if nodeA.MaxDepthInSubtree != 2 {
		t.Errorf("a/ MaxDepthInSubtree = %d, want 2", nodeA.MaxDepthInSubtree)
	}

	// Verify tier tracking
	if !result.TrackTiers {
		t.Error("TrackTiers = false, want true")
	}
	if len(result.PresentTiers) != 3 {
		t.Errorf("len(PresentTiers) = %d, want 3", len(result.PresentTiers))
	}
}

func TestBuildTrieFromSQLiteEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Build trie from empty SQLite
	result, err := BuildTrieFromSQLite(agg)
	if err != nil {
		t.Fatalf("BuildTrieFromSQLite failed: %v", err)
	}

	if len(result.Nodes) != 0 {
		t.Errorf("len(Nodes) = %d, want 0", len(result.Nodes))
	}
}

func TestIsAncestor(t *testing.T) {
	tests := []struct {
		ancestor   string
		descendant string
		want       bool
	}{
		{"", "a/", true},       // root is ancestor of everything
		{"", "a/b/c/", true},   // root is ancestor of everything
		{"a/", "a/b/", true},   // a/ is ancestor of a/b/
		{"a/", "a/b/c/", true}, // a/ is ancestor of a/b/c/
		{"a/b/", "a/", false},  // a/b/ is NOT ancestor of a/
		{"a/", "b/", false},    // a/ is NOT ancestor of b/
		{"a/", "a/", false},    // a/ is NOT ancestor of itself
		{"abc/", "ab/", false}, // abc/ is NOT ancestor of ab/
	}

	for _, tt := range tests {
		got := isAncestor(tt.ancestor, tt.descendant)
		if got != tt.want {
			t.Errorf("isAncestor(%q, %q) = %v, want %v", tt.ancestor, tt.descendant, got, tt.want)
		}
	}
}

func TestTrieNodeOrder(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add objects in a way that tests the lexicographic ordering
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	testObjects := []string{
		"z/file.txt",
		"a/file.txt",
		"m/file.txt",
		"a/b/file.txt",
	}

	for _, key := range testObjects {
		if err := agg.AddObject(key, 100, tiers.Standard); err != nil {
			t.Fatalf("AddObject(%s) failed: %v", key, err)
		}
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	result, err := BuildTrieFromSQLite(agg)
	if err != nil {
		t.Fatalf("BuildTrieFromSQLite failed: %v", err)
	}

	// Verify nodes are in lexicographic order by Pos
	// Expected order: "", "a/", "a/b/", "m/", "z/"
	expectedOrder := []string{"", "a/", "a/b/", "m/", "z/"}
	if len(result.Nodes) != len(expectedOrder) {
		t.Fatalf("len(Nodes) = %d, want %d", len(result.Nodes), len(expectedOrder))
	}

	for i, node := range result.Nodes {
		if node.Prefix != expectedOrder[i] {
			t.Errorf("Nodes[%d].Prefix = %q, want %q", i, node.Prefix, expectedOrder[i])
		}
		if node.Pos != uint64(i) {
			t.Errorf("Nodes[%d].Pos = %d, want %d", i, node.Pos, i)
		}
	}
}
