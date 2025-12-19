package sqliteagg

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestOpenClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify the database file was created
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("database file not created: %v", err)
	}
}

func TestChunkTracking(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Check chunk that doesn't exist
	done, err := agg.ChunkDone("chunk1")
	if err != nil {
		t.Fatalf("ChunkDone failed: %v", err)
	}
	if done {
		t.Error("ChunkDone returned true for non-existent chunk")
	}

	// Begin and mark chunk as done
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}
	if err := agg.MarkChunkDone("chunk1"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check chunk exists now
	done, err = agg.ChunkDone("chunk1")
	if err != nil {
		t.Fatalf("ChunkDone failed: %v", err)
	}
	if !done {
		t.Error("ChunkDone returned false for processed chunk")
	}

	// Check chunk count
	count, err := agg.ChunkCount()
	if err != nil {
		t.Fatalf("ChunkCount failed: %v", err)
	}
	if count != 1 {
		t.Errorf("ChunkCount = %d, want 1", count)
	}
}

func TestChunkRollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Begin, mark chunk, but rollback
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}
	if err := agg.MarkChunkDone("chunk1"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Check chunk should NOT exist after rollback
	done, err := agg.ChunkDone("chunk1")
	if err != nil {
		t.Fatalf("ChunkDone failed: %v", err)
	}
	if done {
		t.Error("ChunkDone returned true after rollback")
	}
}

func TestAddObject(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add some objects
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
		{"b/file4.txt", 400, tiers.StandardIA},
	}

	for _, obj := range testObjects {
		if err := agg.AddObject(obj.key, obj.size, obj.tierID); err != nil {
			t.Fatalf("AddObject(%s) failed: %v", obj.key, err)
		}
	}

	if err := agg.MarkChunkDone("chunk1"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify prefix count
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount failed: %v", err)
	}
	// Expected prefixes: "", "a/", "a/b/", "b/"
	if count != 4 {
		t.Errorf("PrefixCount = %d, want 4", count)
	}

	// Verify max depth
	maxDepth, err := agg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth failed: %v", err)
	}
	if maxDepth != 2 {
		t.Errorf("MaxDepth = %d, want 2", maxDepth)
	}
}

func TestIteratePrefixes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add objects
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("b/file2.txt", 200, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Iterate and verify order
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
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
		t.Fatalf("got %d prefixes, want %d", len(gotPrefixes), len(expectedPrefixes))
	}

	for i, prefix := range gotPrefixes {
		if prefix != expectedPrefixes[i] {
			t.Errorf("prefix[%d] = %q, want %q", i, prefix, expectedPrefixes[i])
		}
	}
}

func TestPrefixAggregation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add objects with same prefix multiple times
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("a/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("a/file3.txt", 50, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify aggregation
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
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
			// Check tier-specific counts
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

	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}
}

func TestPresentTiers(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Add objects with different tiers
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("b/file2.txt", 200, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("c/file3.txt", 300, tiers.DeepArchive); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check present tiers
	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers failed: %v", err)
	}

	// Should have 3 tiers present
	if len(present) != 3 {
		t.Errorf("len(present) = %d, want 3", len(present))
	}

	// Check specific tiers are present
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

func TestResumeAcrossSessions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First session: add some data
	{
		agg, err := Open(DefaultConfig(dbPath))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		if err := agg.BeginChunk(); err != nil {
			t.Fatalf("BeginChunk failed: %v", err)
		}
		if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
		if err := agg.MarkChunkDone("chunk1"); err != nil {
			t.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if err := agg.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// Second session: verify data persisted and add more
	{
		agg, err := Open(DefaultConfig(dbPath))
		if err != nil {
			t.Fatalf("Open (second) failed: %v", err)
		}
		defer agg.Close()

		// Verify chunk1 is done
		done, err := agg.ChunkDone("chunk1")
		if err != nil {
			t.Fatalf("ChunkDone failed: %v", err)
		}
		if !done {
			t.Error("chunk1 should be done after reopening")
		}

		// Verify prefix data persisted
		count, err := agg.PrefixCount()
		if err != nil {
			t.Fatalf("PrefixCount failed: %v", err)
		}
		if count != 2 { // "" and "a/"
			t.Errorf("PrefixCount = %d, want 2", count)
		}

		// Add more data
		if err := agg.BeginChunk(); err != nil {
			t.Fatalf("BeginChunk failed: %v", err)
		}
		if err := agg.AddObject("b/file2.txt", 200, tiers.Standard); err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
		if err := agg.MarkChunkDone("chunk2"); err != nil {
			t.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify combined data
		count, err = agg.PrefixCount()
		if err != nil {
			t.Fatalf("PrefixCount failed: %v", err)
		}
		if count != 3 { // "", "a/", "b/"
			t.Errorf("PrefixCount = %d, want 3", count)
		}
	}
}

func TestExtractPrefixes(t *testing.T) {
	tests := []struct {
		key      string
		expected []string
	}{
		{"file.txt", nil},
		{"a/file.txt", []string{"a/"}},
		{"a/b/file.txt", []string{"a/", "a/b/"}},
		{"a/b/c/file.txt", []string{"a/", "a/b/", "a/b/c/"}},
		{"a/b/c/", []string{"a/", "a/b/", "a/b/c/"}},
		{"", nil},
	}

	for _, tt := range tests {
		got := extractPrefixes(tt.key)
		if len(got) != len(tt.expected) {
			t.Errorf("extractPrefixes(%q) = %v, want %v", tt.key, got, tt.expected)
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("extractPrefixes(%q)[%d] = %q, want %q", tt.key, i, got[i], tt.expected[i])
			}
		}
	}
}

// TestMultiChunkAggregation verifies that stats from multiple chunks are
// correctly accumulated without double-counting.
func TestMultiChunkAggregation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// Chunk 1: objects under a/
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}
	if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("a/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.MarkChunkDone("chunk1"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Chunk 2: more objects under a/ and new prefix b/
	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}
	if err := agg.AddObject("a/file3.txt", 300, tiers.GlacierFR); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("b/file1.txt", 400, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.MarkChunkDone("chunk2"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify stats are correctly aggregated
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
	}
	defer iter.Close()

	prefixStats := make(map[string]PrefixRow)
	for iter.Next() {
		row := iter.Row()
		prefixStats[row.Prefix] = row
	}
	if iter.Err() != nil {
		t.Fatalf("Iterator error: %v", iter.Err())
	}

	// Verify root prefix stats
	root := prefixStats[""]
	if root.TotalCount != 4 {
		t.Errorf("root TotalCount = %d, want 4", root.TotalCount)
	}
	if root.TotalBytes != 1000 {
		t.Errorf("root TotalBytes = %d, want 1000", root.TotalBytes)
	}

	// Verify a/ prefix has combined stats from both chunks
	aPrefix := prefixStats["a/"]
	if aPrefix.TotalCount != 3 {
		t.Errorf("a/ TotalCount = %d, want 3", aPrefix.TotalCount)
	}
	if aPrefix.TotalBytes != 600 {
		t.Errorf("a/ TotalBytes = %d, want 600", aPrefix.TotalBytes)
	}
	if aPrefix.TierCounts[tiers.Standard] != 2 {
		t.Errorf("a/ Standard count = %d, want 2", aPrefix.TierCounts[tiers.Standard])
	}
	if aPrefix.TierCounts[tiers.GlacierFR] != 1 {
		t.Errorf("a/ GlacierFR count = %d, want 1", aPrefix.TierCounts[tiers.GlacierFR])
	}

	// Verify b/ prefix stats
	bPrefix := prefixStats["b/"]
	if bPrefix.TotalCount != 1 {
		t.Errorf("b/ TotalCount = %d, want 1", bPrefix.TotalCount)
	}
	if bPrefix.TotalBytes != 400 {
		t.Errorf("b/ TotalBytes = %d, want 400", bPrefix.TotalBytes)
	}
}

// TestIdempotentChunkProcessing verifies that attempting to re-process
// an already-done chunk does not modify stats.
func TestIdempotentChunkProcessing(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First session: process chunk1
	{
		agg, err := Open(DefaultConfig(dbPath))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		if err := agg.BeginChunk(); err != nil {
			t.Fatalf("BeginChunk failed: %v", err)
		}
		if err := agg.AddObject("a/file1.txt", 100, tiers.Standard); err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
		if err := agg.MarkChunkDone("chunk1"); err != nil {
			t.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if err := agg.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// Second session: verify chunk1 is done and stats unchanged if we skip it
	{
		agg, err := Open(DefaultConfig(dbPath))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer agg.Close()

		// Check if chunk1 is already done (simulating resume logic)
		done, err := agg.ChunkDone("chunk1")
		if err != nil {
			t.Fatalf("ChunkDone failed: %v", err)
		}
		if !done {
			t.Fatal("chunk1 should be marked as done")
		}

		// Get stats before any re-processing attempt
		iter, err := agg.IteratePrefixes()
		if err != nil {
			t.Fatalf("IteratePrefixes failed: %v", err)
		}

		var rootBefore PrefixRow
		for iter.Next() {
			row := iter.Row()
			if row.Prefix == "" {
				rootBefore = row
				break
			}
		}
		iter.Close()

		// Verify count and bytes are correct (1 object, 100 bytes)
		if rootBefore.TotalCount != 1 {
			t.Errorf("root TotalCount = %d, want 1", rootBefore.TotalCount)
		}
		if rootBefore.TotalBytes != 100 {
			t.Errorf("root TotalBytes = %d, want 100", rootBefore.TotalBytes)
		}

		// Process a new chunk (chunk2)
		if err := agg.BeginChunk(); err != nil {
			t.Fatalf("BeginChunk failed: %v", err)
		}
		if err := agg.AddObject("b/file1.txt", 200, tiers.Standard); err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
		if err := agg.MarkChunkDone("chunk2"); err != nil {
			t.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify final stats
		iter2, err := agg.IteratePrefixes()
		if err != nil {
			t.Fatalf("IteratePrefixes failed: %v", err)
		}
		defer iter2.Close()

		var rootAfter PrefixRow
		for iter2.Next() {
			row := iter2.Row()
			if row.Prefix == "" {
				rootAfter = row
				break
			}
		}

		// Should have 2 objects, 300 bytes total (not 3 objects if chunk1 was re-processed)
		if rootAfter.TotalCount != 2 {
			t.Errorf("root TotalCount after = %d, want 2", rootAfter.TotalCount)
		}
		if rootAfter.TotalBytes != 300 {
			t.Errorf("root TotalBytes after = %d, want 300", rootAfter.TotalBytes)
		}
	}
}

// TestDeepHierarchy verifies correct handling of deeply nested prefixes.
func TestDeepHierarchy(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	// Add deeply nested objects
	if err := agg.AddObject("a/b/c/d/e/file1.txt", 100, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("a/b/c/d/e/file2.txt", 200, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if err := agg.AddObject("a/b/c/other.txt", 50, tiers.Standard); err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify max depth
	maxDepth, err := agg.MaxDepth()
	if err != nil {
		t.Fatalf("MaxDepth failed: %v", err)
	}
	if maxDepth != 5 {
		t.Errorf("MaxDepth = %d, want 5", maxDepth)
	}

	// Verify prefix counts at different levels
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
	}
	defer iter.Close()

	prefixStats := make(map[string]PrefixRow)
	for iter.Next() {
		row := iter.Row()
		prefixStats[row.Prefix] = row
	}

	// a/b/c/ should have 3 objects (2 deep + 1 at c level)
	if prefixStats["a/b/c/"].TotalCount != 3 {
		t.Errorf("a/b/c/ TotalCount = %d, want 3", prefixStats["a/b/c/"].TotalCount)
	}

	// a/b/c/d/e/ should have 2 objects
	if prefixStats["a/b/c/d/e/"].TotalCount != 2 {
		t.Errorf("a/b/c/d/e/ TotalCount = %d, want 2", prefixStats["a/b/c/d/e/"].TotalCount)
	}

	// Verify depths are correct
	if prefixStats["a/"].Depth != 1 {
		t.Errorf("a/ depth = %d, want 1", prefixStats["a/"].Depth)
	}
	if prefixStats["a/b/c/d/e/"].Depth != 5 {
		t.Errorf("a/b/c/d/e/ depth = %d, want 5", prefixStats["a/b/c/d/e/"].Depth)
	}
}

// TestEmptyDatabase verifies correct handling of empty database.
func TestEmptyDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	// No data added - verify counts are zero
	count, err := agg.PrefixCount()
	if err != nil {
		t.Fatalf("PrefixCount failed: %v", err)
	}
	if count != 0 {
		t.Errorf("PrefixCount = %d, want 0", count)
	}

	chunkCount, err := agg.ChunkCount()
	if err != nil {
		t.Fatalf("ChunkCount failed: %v", err)
	}
	if chunkCount != 0 {
		t.Errorf("ChunkCount = %d, want 0", chunkCount)
	}

	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers failed: %v", err)
	}
	if len(present) != 0 {
		t.Errorf("len(present) = %d, want 0", len(present))
	}
}

// TestAllTiers verifies handling of all supported storage tiers.
func TestAllTiers(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	agg, err := Open(DefaultConfig(dbPath))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	// Add one object for each tier
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
			t.Fatalf("AddObject(%s, tier=%d) failed: %v", key, tierID, err)
		}
	}

	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all tiers are present
	present, err := agg.PresentTiers()
	if err != nil {
		t.Fatalf("PresentTiers failed: %v", err)
	}
	if len(present) != len(allTiers) {
		t.Errorf("len(present) = %d, want %d", len(present), len(allTiers))
	}

	// Verify tier-specific stats in iteration
	iter, err := agg.IteratePrefixes()
	if err != nil {
		t.Fatalf("IteratePrefixes failed: %v", err)
	}
	defer iter.Close()

	for iter.Next() {
		row := iter.Row()
		if row.Prefix == "data/" {
			// Verify each tier has exactly 1 object
			for _, tierID := range allTiers {
				if row.TierCounts[tierID] != 1 {
					t.Errorf("data/ tier %d count = %d, want 1", tierID, row.TierCounts[tierID])
				}
			}
		}
	}
}
