package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// createTestRunFile creates a compressed run file with the given prefixes.
func createTestRunFile(t *testing.T, dir, name string, prefixes []string, countOffset int) string {
	t.Helper()
	path := filepath.Join(dir, name)

	writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("create run file %s: %v", name, err)
	}

	for i, prefix := range prefixes {
		row := &PrefixRow{
			Prefix:     prefix,
			Depth:      uint16(len(prefix) - len(prefix[:len(prefix)-1]) + 1),
			Count:      uint64(countOffset + i + 1),
			TotalBytes: uint64((countOffset + i + 1) * 100),
		}
		row.TierCounts[tiers.Standard] = row.Count
		row.TierBytes[tiers.Standard] = row.TotalBytes
		if err := writer.Write(row); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return path
}

func TestParallelMerger_TwoFiles(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	// Create two sorted run files with interleaved prefixes
	prefixes1 := []string{"a/", "c/", "e/", "g/"}
	prefixes2 := []string{"b/", "d/", "f/", "h/"}

	path1 := createTestRunFile(t, tmpDir, "run1.crun", prefixes1, 0)
	path2 := createTestRunFile(t, tmpDir, "run2.crun", prefixes2, 10)

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:     2,
		MaxFanIn:       4,
		TempDir:        tmpDir,
		UseCompression: true,
	})

	outputPath, err := merger.MergeAll(ctx, []string{path1, path2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Read and verify output
	reader, err := OpenCompressedRunFile(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	expectedPrefixes := []string{"a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/"}
	if reader.Count() != uint64(len(expectedPrefixes)) {
		t.Errorf("expected %d records, got %d", len(expectedPrefixes), reader.Count())
	}

	for i, expected := range expectedPrefixes {
		row, err := reader.Read()
		if err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
		if row.Prefix != expected {
			t.Errorf("row %d: expected prefix %q, got %q", i, expected, row.Prefix)
		}
	}
}

func TestParallelMerger_DuplicatePrefixes(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	// Create two run files with overlapping prefixes
	prefixes1 := []string{"a/", "b/", "c/"}
	prefixes2 := []string{"b/", "c/", "d/"}

	path1 := createTestRunFile(t, tmpDir, "run1.crun", prefixes1, 0)
	path2 := createTestRunFile(t, tmpDir, "run2.crun", prefixes2, 10)

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir:        tmpDir,
		UseCompression: true,
	})

	outputPath, err := merger.MergeAll(ctx, []string{path1, path2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	reader, err := OpenCompressedRunFile(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	// Should have 4 unique prefixes: a/, b/, c/, d/
	if reader.Count() != 4 {
		t.Errorf("expected 4 unique prefixes, got %d", reader.Count())
	}

	// Verify b/ and c/ have merged counts
	var bRow, cRow *PrefixRow
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if row.Prefix == "b/" {
			bRow = row
		}
		if row.Prefix == "c/" {
			cRow = row
		}
	}

	// b/ appears in both files: countOffset=0,idx=1 -> 2 and countOffset=10,idx=0 -> 11, sum=13
	if bRow != nil && bRow.Count != 13 {
		t.Errorf("b/ expected merged count 13, got %d", bRow.Count)
	}

	// c/ appears in both files: countOffset=0,idx=2 -> 3 and countOffset=10,idx=1 -> 12, sum=15
	if cRow != nil && cRow.Count != 15 {
		t.Errorf("c/ expected merged count 15, got %d", cRow.Count)
	}
}

func TestParallelMerger_ManyFiles(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	const numFiles = 20
	const prefixesPerFile = 50

	paths := make([]string, 0, numFiles)
	allPrefixes := make([]string, 0, numFiles*prefixesPerFile)

	for i := range numFiles {
		var prefixes []string
		for j := range prefixesPerFile {
			prefix := fmt.Sprintf("file%02d/prefix%04d/", i, j)
			prefixes = append(prefixes, prefix)
			allPrefixes = append(allPrefixes, prefix)
		}
		path := createTestRunFile(t, tmpDir, fmt.Sprintf("run%02d.crun", i), prefixes, i*100)
		paths = append(paths, path)
	}

	// Sort expected prefixes
	sort.Strings(allPrefixes)

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:     4,
		MaxFanIn:       4,
		TempDir:        tmpDir,
		UseCompression: true,
	})

	outputPath, err := merger.MergeAll(ctx, paths)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	defer func() { merger.CleanupIntermediateFiles() }()

	reader, err := OpenCompressedRunFile(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	if reader.Count() != uint64(len(allPrefixes)) {
		t.Errorf("expected %d records, got %d", len(allPrefixes), reader.Count())
	}

	// Verify sorted order
	prevPrefix := ""
	count := 0
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if row.Prefix < prevPrefix {
			t.Errorf("out of order: %q < %q", row.Prefix, prevPrefix)
		}
		prevPrefix = row.Prefix
		count++
	}

	if count != len(allPrefixes) {
		t.Errorf("read %d records, expected %d", count, len(allPrefixes))
	}

	// Verify statistics
	rounds, _, _ := merger.Statistics()
	if rounds < 2 {
		t.Logf("Merge completed in %d rounds (20 files with fanIn=4 should need ~2 rounds)", rounds)
	}
}

func TestParallelMerger_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	path := createTestRunFile(t, tmpDir, "single.crun", []string{"a/", "b/", "c/"}, 0)

	merger := NewParallelMerger(DefaultParallelMergeConfig())

	outputPath, err := merger.MergeAll(ctx, []string{path})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Single file should be returned as-is
	if outputPath != path {
		t.Errorf("expected same path for single file, got %q instead of %q", outputPath, path)
	}
}

func TestParallelMerger_EmptyInput(t *testing.T) {
	merger := NewParallelMerger(DefaultParallelMergeConfig())

	_, err := merger.MergeAll(context.Background(), []string{})
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestParallelMerger_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	// Create enough files to ensure we have multiple rounds
	const numFiles = 10
	paths := make([]string, 0, numFiles)
	for i := range numFiles {
		prefixes := make([]string, 100)
		for j := range prefixes {
			prefixes[j] = fmt.Sprintf("prefix%05d/", i*100+j)
		}
		path := createTestRunFile(t, tmpDir, fmt.Sprintf("run%02d.crun", i), prefixes, 0)
		paths = append(paths, path)
	}

	// Cancel immediately
	cancel()

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers: 2,
		MaxFanIn:   2,
		TempDir:    tmpDir,
	})

	_, err := merger.MergeAll(ctx, paths)
	if err == nil {
		t.Error("expected error on cancelled context")
	}
}

func TestParallelMerger_UncompressedOutput(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	path1 := createTestRunFile(t, tmpDir, "run1.crun", []string{"a/", "c/"}, 0)
	path2 := createTestRunFile(t, tmpDir, "run2.crun", []string{"b/", "d/"}, 0)

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir:        tmpDir,
		UseCompression: false, // Uncompressed output
	})

	outputPath, err := merger.MergeAll(ctx, []string{path1, path2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Read back with auto-detect (should work with uncompressed)
	reader, err := OpenRunFileAuto(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	if reader.Count() != 4 {
		t.Errorf("expected 4 records, got %d", reader.Count())
	}
}

func TestParallelMerger_MixedInputFormats(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	// Create one compressed and one uncompressed run file
	compressedPath := filepath.Join(tmpDir, "compressed.crun")
	uncompressedPath := filepath.Join(tmpDir, "uncompressed.run")

	// Compressed file
	cw, _ := NewCompressedRunWriter(compressedPath, CompressedRunWriterOptions{})
	cw.Write(&PrefixRow{Prefix: "a/", Count: 1})
	cw.Write(&PrefixRow{Prefix: "c/", Count: 3})
	cw.Close()

	// Uncompressed file
	uw, _ := NewRunFileWriter(uncompressedPath, 0)
	uw.Write(&PrefixRow{Prefix: "b/", Count: 2})
	uw.Write(&PrefixRow{Prefix: "d/", Count: 4})
	uw.Close()

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir:        tmpDir,
		UseCompression: true,
	})

	outputPath, err := merger.MergeAll(ctx, []string{compressedPath, uncompressedPath})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	reader, err := OpenCompressedRunFile(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	expected := []string{"a/", "b/", "c/", "d/"}
	for i, exp := range expected {
		row, err := reader.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if row.Prefix != exp {
			t.Errorf("row %d: expected %q, got %q", i, exp, row.Prefix)
		}
	}
}

func TestParallelMerger_TierDataPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	// Create run files with tier data
	path1 := filepath.Join(tmpDir, "run1.crun")
	path2 := filepath.Join(tmpDir, "run2.crun")

	w1, _ := NewCompressedRunWriter(path1, CompressedRunWriterOptions{})
	row1 := &PrefixRow{Prefix: "data/", Count: 10, TotalBytes: 1000}
	row1.TierCounts[tiers.Standard] = 5
	row1.TierCounts[tiers.GlacierFR] = 5
	row1.TierBytes[tiers.Standard] = 500
	row1.TierBytes[tiers.GlacierFR] = 500
	w1.Write(row1)
	w1.Close()

	w2, _ := NewCompressedRunWriter(path2, CompressedRunWriterOptions{})
	row2 := &PrefixRow{Prefix: "data/", Count: 20, TotalBytes: 2000}
	row2.TierCounts[tiers.Standard] = 15
	row2.TierCounts[tiers.GlacierIR] = 5
	row2.TierBytes[tiers.Standard] = 1500
	row2.TierBytes[tiers.GlacierIR] = 500
	w2.Write(row2)
	w2.Close()

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir:        tmpDir,
		UseCompression: true,
	})

	outputPath, err := merger.MergeAll(ctx, []string{path1, path2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	reader, err := OpenCompressedRunFile(outputPath, 0)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer reader.Close()

	row, _ := reader.Read()

	// Verify merged counts
	if row.Count != 30 {
		t.Errorf("expected merged count 30, got %d", row.Count)
	}
	if row.TotalBytes != 3000 {
		t.Errorf("expected merged bytes 3000, got %d", row.TotalBytes)
	}

	// Verify tier data merged correctly
	if row.TierCounts[tiers.Standard] != 20 { // 5 + 15
		t.Errorf("expected Standard tier count 20, got %d", row.TierCounts[tiers.Standard])
	}
	if row.TierCounts[tiers.GlacierFR] != 5 {
		t.Errorf("expected GlacierFR tier count 5, got %d", row.TierCounts[tiers.GlacierFR])
	}
	if row.TierCounts[tiers.GlacierIR] != 5 {
		t.Errorf("expected GlacierIR tier count 5, got %d", row.TierCounts[tiers.GlacierIR])
	}
	if row.TierBytes[tiers.Standard] != 2000 { // 500 + 1500
		t.Errorf("expected Standard tier bytes 2000, got %d", row.TierBytes[tiers.Standard])
	}
}

func TestPartitionPaths(t *testing.T) {
	merger := NewParallelMerger(ParallelMergeConfig{
		MaxFanIn: 4,
	})

	tests := []struct {
		name     string
		paths    []string
		expected [][]string
	}{
		{
			name:     "exact multiple",
			paths:    []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			expected: [][]string{{"a", "b", "c", "d"}, {"e", "f", "g", "h"}},
		},
		{
			name:     "with remainder",
			paths:    []string{"a", "b", "c", "d", "e"},
			expected: [][]string{{"a", "b", "c", "d"}, {"e"}},
		},
		{
			name:     "less than fanIn",
			paths:    []string{"a", "b"},
			expected: [][]string{{"a", "b"}},
		},
		{
			name:     "single",
			paths:    []string{"a"},
			expected: [][]string{{"a"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := merger.partitionPaths(tt.paths)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d groups, got %d", len(tt.expected), len(result))
			}
			for i := range result {
				if len(result[i]) != len(tt.expected[i]) {
					t.Errorf("group %d: expected %d items, got %d", i, len(tt.expected[i]), len(result[i]))
				}
			}
		})
	}
}
