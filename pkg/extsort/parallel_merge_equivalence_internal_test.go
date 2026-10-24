package extsort

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

// TestParallelMerge_EquivalentToSerial verifies that the parallel
// multi-round merger yields exactly the rows of a single-pass serial
// k-way merge of the same input, across fan-ins that stream the runs
// directly, plan partial rounds, and merge everything over several
// rounds. Prefixes overlap between runs, so rows are summed across
// rounds as well as within one.
func TestParallelMerge_EquivalentToSerial(t *testing.T) {
	for _, c := range []struct {
		name       string
		runCount   int
		rowsPerRun int
		maxFanIn   int
		finalFanIn int
		wantRounds int
	}{
		{name: "few_runs", runCount: 4, rowsPerRun: 100, maxFanIn: 4, wantRounds: 0},
		{name: "streamed_only", runCount: 17, rowsPerRun: 50, maxFanIn: 4, finalFanIn: 64, wantRounds: 0},
		{name: "one_partial_round", runCount: 17, rowsPerRun: 50, maxFanIn: 4, finalFanIn: 8, wantRounds: 1},
		{name: "full_then_partial", runCount: 17, rowsPerRun: 50, maxFanIn: 4, finalFanIn: 4, wantRounds: 2},
		{name: "three_rounds", runCount: 65, rowsPerRun: 25, maxFanIn: 4, finalFanIn: 4, wantRounds: 3},
		{name: "fan_in_two", runCount: 65, rowsPerRun: 25, maxFanIn: 2, finalFanIn: 2, wantRounds: 6},
		{name: "wide_final", runCount: 100, rowsPerRun: 10, maxFanIn: 4, finalFanIn: 8, wantRounds: 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			runEquivalenceTest(t, c.runCount, c.rowsPerRun, c.maxFanIn, c.finalFanIn, c.wantRounds)
		})
	}
}

func runEquivalenceTest(t *testing.T, runCount, rowsPerRun, maxFanIn, finalFanIn, wantRounds int) {
	t.Helper()
	dir := t.TempDir()

	paths := make([]string, runCount)
	for i := range runCount {
		// Strictly increasing per run, overlapping across runs.
		prefixes := make([]string, rowsPerRun)
		for j := range rowsPerRun {
			prefixes[j] = fmt.Sprintf("p/%06d/", j*(i%3+1)+i%5)
		}
		paths[i] = createTestRunFile(t, dir, fmt.Sprintf("input_%04d.crun", i), prefixes, i*rowsPerRun)
	}

	// The merger deletes its inputs, so the serial merge reads them first.
	serialOut := drainSerial(t, paths)

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:       4,
		MaxFanIn:         maxFanIn,
		FinalFanIn:       finalFanIn,
		BufferSize:       64 * 1024,
		TempDir:          t.TempDir(),
		UseCompression:   true,
		CompressionLevel: CompressionFastest,
	})
	parallelOut := mergeAllRows(t, merger, paths)

	if rounds := merger.Statistics().Rounds; rounds != wantRounds {
		t.Errorf("disk rounds = %d, want %d", rounds, wantRounds)
	}
	if len(serialOut) != len(parallelOut) {
		t.Fatalf("row count mismatch: serial=%d parallel=%d", len(serialOut), len(parallelOut))
	}
	for i := range serialOut {
		if serialOut[i] != parallelOut[i] {
			t.Fatalf("row %d differs:\nserial   %+v\nparallel %+v", i, serialOut[i], parallelOut[i])
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
	iter, err := newMergeIterator(readers)
	if err != nil {
		t.Fatalf("newMergeIterator: %v", err)
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
