package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"testing"
)

// TestParallelMerge_EquivalentToSerial verifies that the parallel
// multi-round merger produces byte-identical output to a single-pass
// serial k-way merge for the same input. Without this, subtle
// reordering bugs in the parallel path would slip past the existing
// per-test assertions.
func TestParallelMerge_EquivalentToSerial(t *testing.T) {
	for _, c := range []struct {
		name       string
		runCount   int
		rowsPerRun int
		maxFanIn   int
	}{
		{name: "few_runs", runCount: 4, rowsPerRun: 100, maxFanIn: 4},
		{name: "two_round_merge", runCount: 17, rowsPerRun: 50, maxFanIn: 4},
		{name: "three_round_merge", runCount: 65, rowsPerRun: 25, maxFanIn: 4},
	} {
		t.Run(c.name, func(t *testing.T) {
			runEquivalenceTest(t, c.runCount, c.rowsPerRun, c.maxFanIn)
		})
	}
}

func runEquivalenceTest(t *testing.T, runCount, rowsPerRun, maxFanIn int) {
	t.Helper()
	dir := t.TempDir()

	paths := make([]string, runCount)
	for i := range runCount {
		// Each run holds rowsPerRun unique prefixes; runs are
		// individually sorted, prefixes don't overlap so de-dup logic
		// stays out of the equivalence picture for now.
		prefixes := make([]string, rowsPerRun)
		for j := range rowsPerRun {
			// Pad to fixed width so lexicographic order matches numeric.
			prefixes[j] = fmt.Sprintf("run%04d/key%06d/", i, j)
		}
		paths[i] = createTestRunFile(t, dir, fmt.Sprintf("input_%04d.crun", i), prefixes, i*rowsPerRun)
	}

	serialOut := drainSerial(t, paths)

	parallelDir := t.TempDir()
	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:       4,
		MaxFanIn:         maxFanIn,
		BufferSize:       64 * 1024,
		TempDir:          parallelDir,
		UseCompression:   true,
		CompressionLevel: CompressionFastest,
	})
	finalPath, err := merger.MergeAll(context.Background(), paths)
	if err != nil {
		t.Fatalf("MergeAll: %v", err)
	}
	defer func() {
		_ = merger.CleanupIntermediateFiles()
	}()

	parallelOut := drainOne(t, finalPath)

	if len(serialOut) != len(parallelOut) {
		t.Fatalf("row count mismatch: serial=%d parallel=%d", len(serialOut), len(parallelOut))
	}
	for i := range serialOut {
		if serialOut[i].Prefix != parallelOut[i].Prefix {
			t.Fatalf("row %d prefix mismatch: serial=%q parallel=%q", i, serialOut[i].Prefix, parallelOut[i].Prefix)
		}
		if serialOut[i].Count != parallelOut[i].Count {
			t.Fatalf("row %d count mismatch at prefix %q: serial=%d parallel=%d",
				i, serialOut[i].Prefix, serialOut[i].Count, parallelOut[i].Count)
		}
		if serialOut[i].TotalBytes != parallelOut[i].TotalBytes {
			t.Fatalf("row %d totalbytes mismatch at prefix %q", i, serialOut[i].Prefix)
		}
	}
}

func drainSerial(t *testing.T, paths []string) []PrefixRow {
	t.Helper()
	readers := make([]RunReader, 0, len(paths))
	for _, p := range paths {
		r, err := OpenRunFileAuto(p, 64*1024)
		if err != nil {
			t.Fatalf("OpenRunFileAuto %s: %v", filepath.Base(p), err)
		}
		readers = append(readers, r)
	}
	iter, err := newMergeIteratorFromRunReaders(readers)
	if err != nil {
		t.Fatalf("newMergeIteratorFromRunReaders: %v", err)
	}
	defer iter.Close()
	var out []PrefixRow
	for {
		row, err := iter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("serial Next: %v", err)
		}
		out = append(out, *row)
	}

	return out
}

func drainOne(t *testing.T, path string) []PrefixRow {
	t.Helper()
	reader, err := OpenRunFileAuto(path, 64*1024)
	if err != nil {
		t.Fatalf("OpenRunFileAuto %s: %v", filepath.Base(path), err)
	}
	defer reader.Close()
	var out []PrefixRow
	for {
		var row PrefixRow
		err := reader.ReadInto(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		out = append(out, row)
	}

	return out
}

// suppress "unused import" if strconv falls out of generation later.
var _ = strconv.Itoa
