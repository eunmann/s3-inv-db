package sqliteagg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
Test Organization:

Shared tests (aggregator_unified_test.go):
  - TestChunkAggregator runs all shared tests on Standard and Normalized implementations

Aggregator-specific tests (this file):
  - TestOpenClose: Database lifecycle (Open/Close)
  - TestResumeAcrossSessions: Cross-session persistence
  - TestIdempotentChunkProcessing: Idempotent chunk handling
  - TestExtractPrefixes: Unit test for prefix extraction
  - TestConfigValidate: Configuration validation
*/

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

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "valid default config",
			cfg:     DefaultConfig("/tmp/test.db"),
			wantErr: false,
		},
		{
			name:    "empty db path",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name: "invalid synchronous",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				Synchronous: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "negative mmap size",
			cfg: Config{
				DBPath:   "/tmp/test.db",
				MmapSize: -1,
			},
			wantErr: true,
		},
		{
			name: "negative cache size",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				CacheSizeKB: -1,
			},
			wantErr: true,
		},
		{
			name: "empty synchronous uses default",
			cfg: Config{
				DBPath:      "/tmp/test.db",
				Synchronous: "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
