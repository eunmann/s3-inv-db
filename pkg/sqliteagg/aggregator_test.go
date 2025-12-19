package sqliteagg

import (
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
