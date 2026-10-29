package extsort

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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
		row.SetTier(tiers.Standard, row.Count, row.TotalBytes)
		if err := writer.Write(row); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return path
}

// mergeAllRows merges paths with m, copies every row out of the
// iterator, and runs the cleanup.
func mergeAllRows(t *testing.T, m *ParallelMerger, paths []string) []PrefixRow {
	t.Helper()
	iter, cleanup, err := m.MergeAllToIterator(t.Context(), paths)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	var rows []PrefixRow
	for {
		row, err := iter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cleanup()
			t.Fatalf("next: %v", err)
		}
		rows = append(rows, *row)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	return rows
}

func rowPrefixes(rows []PrefixRow) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].Prefix
	}

	return out
}

// dirFiles lists the names of the files in dir.
func dirFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	return names
}

// createSpreadRuns writes n runs of 50 unique prefixes each to dir.
func createSpreadRuns(t *testing.T, dir string, n int) []string {
	t.Helper()
	paths := make([]string, n)
	for i := range n {
		prefixes := make([]string, 50)
		for j := range prefixes {
			prefixes[j] = fmt.Sprintf("file%02d/prefix%04d/", i, j)
		}
		paths[i] = createTestRunFile(t, dir, fmt.Sprintf("run%02d.crun", i), prefixes, i*100)
	}

	return paths
}

func TestParallelMerger_TwoFiles(t *testing.T) {
	tmpDir := t.TempDir()

	path1 := createTestRunFile(t, tmpDir, "run1.crun", []string{"a/", "c/", "e/", "g/"}, 0)
	path2 := createTestRunFile(t, tmpDir, "run2.crun", []string{"b/", "d/", "f/", "h/"}, 10)

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers: 2,
		MaxFanIn:   4,
		TempDir:    tmpDir,
	})

	got := rowPrefixes(mergeAllRows(t, merger, []string{path1, path2}))
	want := []string{"a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/"}
	if !slices.Equal(got, want) {
		t.Errorf("prefixes = %v, want %v", got, want)
	}
}

func TestParallelMerger_DuplicatePrefixes(t *testing.T) {
	tmpDir := t.TempDir()

	path1 := createTestRunFile(t, tmpDir, "run1.crun", []string{"a/", "b/", "c/"}, 0)
	path2 := createTestRunFile(t, tmpDir, "run2.crun", []string{"b/", "c/", "d/"}, 10)

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir: tmpDir,
	})

	rows := mergeAllRows(t, merger, []string{path1, path2})
	if got, want := rowPrefixes(rows), []string{"a/", "b/", "c/", "d/"}; !slices.Equal(got, want) {
		t.Fatalf("prefixes = %v, want %v", got, want)
	}
	// b/: 2 (run1, idx 1) + 11 (run2, idx 0); c/: 3 + 12.
	if rows[1].Count != 13 {
		t.Errorf("b/ count = %d, want 13", rows[1].Count)
	}
	if rows[2].Count != 15 {
		t.Errorf("c/ count = %d, want 15", rows[2].Count)
	}
}

func TestParallelMerger_ManyFiles(t *testing.T) {
	tmpDir := t.TempDir()
	paths := createSpreadRuns(t, tmpDir, 20)

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers: 4,
		MaxFanIn:   4,
		FinalFanIn: 4,
		TempDir:    tmpDir,
	})

	rows := mergeAllRows(t, merger, paths)
	if len(rows) != 20*50 {
		t.Fatalf("got %d rows, want %d", len(rows), 20*50)
	}
	if !slices.IsSorted(rowPrefixes(rows)) {
		t.Error("merged rows are not sorted")
	}
	if rounds := merger.Statistics().Rounds; rounds < 2 {
		t.Errorf("rounds = %d, want at least 2 for 20 runs at fan-in 4", rounds)
	}
}

func TestParallelMerger_SingleFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := createTestRunFile(t, tmpDir, "single.crun", []string{"a/", "b/", "c/"}, 0)

	merger := NewParallelMerger(ParallelMergeConfig{})

	got := rowPrefixes(mergeAllRows(t, merger, []string{path}))
	if want := []string{"a/", "b/", "c/"}; !slices.Equal(got, want) {
		t.Errorf("prefixes = %v, want %v", got, want)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("input run still exists after cleanup (stat err %v)", err)
	}
}

func TestParallelMerger_EmptyInput(t *testing.T) {
	merger := NewParallelMerger(ParallelMergeConfig{})

	_, _, err := merger.MergeAllToIterator(t.Context(), []string{})
	if !errors.Is(err, ErrNoInputPaths) {
		t.Errorf("err = %v, want ErrNoInputPaths", err)
	}
}

func TestParallelMerger_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())

	paths := createSpreadRuns(t, tmpDir, 10)
	cancel()

	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers: 2,
		MaxFanIn:   2,
		FinalFanIn: 2,
		TempDir:    tmpDir,
	})

	if _, _, err := merger.MergeAllToIterator(ctx, paths); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if left := dirFiles(t, tmpDir); len(left) != 0 {
		t.Errorf("cancelled merge left files: %v", left)
	}
}

// TestParallelMerger_RemovesRunsAsMerged checks that a multi-round
// merge deletes each run once it is merged into a larger one, and that
// the cleanup leaves nothing behind.
func TestParallelMerger_RemovesRunsAsMerged(t *testing.T) {
	tmpDir := t.TempDir()
	paths := createSpreadRuns(t, tmpDir, 20)

	var onDisk []string
	merger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers: 4,
		MaxFanIn:   3,
		FinalFanIn: 3,
		TempDir:    tmpDir,
		OnRoundComplete: func(round, remaining int) {
			if n := len(dirFiles(t, tmpDir)); n != remaining {
				onDisk = append(onDisk, fmt.Sprintf("round %d: %d files, %d runs remaining", round, n, remaining))
			}
		},
	})

	rows := mergeAllRows(t, merger, paths)
	if len(rows) != 20*50 {
		t.Errorf("got %d rows, want %d", len(rows), 20*50)
	}
	if rounds := merger.Statistics().Rounds; rounds < 2 {
		t.Errorf("rounds = %d, want at least 2", rounds)
	}
	for _, msg := range onDisk {
		t.Error(msg)
	}
	if left := dirFiles(t, tmpDir); len(left) != 0 {
		t.Errorf("files left after cleanup: %v", left)
	}
}

// TestParallelMerger_FailureRemovesRuns checks that a merge that fails
// returns an error and deletes every run, inputs and intermediates.
func TestParallelMerger_FailureRemovesRuns(t *testing.T) {
	for _, c := range []struct {
		name       string
		runs       int
		finalFanIn int
		breakRun   func(t *testing.T, path string)
	}{
		{"missing_run_disk_round", 9, 2, func(t *testing.T, path string) {
			t.Helper()
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}},
		{"corrupt_body_disk_round", 9, 2, corruptRunBody},
		{"bad_header_final_merge", 3, 0, func(t *testing.T, path string) {
			t.Helper()
			if err := os.WriteFile(path, []byte("not a run file at all, not even close"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			paths := createSpreadRuns(t, tmpDir, c.runs)
			c.breakRun(t, paths[c.runs/2])

			merger := NewParallelMerger(ParallelMergeConfig{
				NumWorkers: 4,
				MaxFanIn:   2,
				FinalFanIn: c.finalFanIn,
				TempDir:    tmpDir,
			})
			if _, _, err := merger.MergeAllToIterator(t.Context(), paths); err == nil {
				t.Fatal("merge succeeded with a broken run")
			}
			if left := dirFiles(t, tmpDir); len(left) != 0 {
				t.Errorf("failed merge left files: %v", left)
			}
		})
	}
}

// corruptRunBody overwrites a run's compressed body, keeping its header.
func corruptRunBody(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	garbage := bytes.Repeat([]byte{0xA5}, int(info.Size())-compressedRunFileHeader)
	if _, err := f.WriteAt(garbage, compressedRunFileHeader); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallelMerger_TierDataPreserved(t *testing.T) {
	tmpDir := t.TempDir()

	path1 := filepath.Join(tmpDir, "run1.crun")
	path2 := filepath.Join(tmpDir, "run2.crun")

	w1, _ := NewCompressedRunWriter(path1, CompressedRunWriterOptions{})
	row1 := &PrefixRow{Prefix: "data/", Count: 10, TotalBytes: 1000}
	row1.SetTier(tiers.Standard, 5, 500)
	row1.SetTier(tiers.GlacierFR, 5, 500)
	w1.Write(row1)
	w1.Close()

	w2, _ := NewCompressedRunWriter(path2, CompressedRunWriterOptions{})
	row2 := &PrefixRow{Prefix: "data/", Count: 20, TotalBytes: 2000}
	row2.SetTier(tiers.Standard, 15, 1500)
	row2.SetTier(tiers.GlacierIR, 5, 500)
	w2.Write(row2)
	w2.Close()

	merger := NewParallelMerger(ParallelMergeConfig{
		TempDir: tmpDir,
	})

	rows := mergeAllRows(t, merger, []string{path1, path2})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	row := rows[0]

	if row.Count != 30 {
		t.Errorf("expected merged count 30, got %d", row.Count)
	}
	if row.TotalBytes != 3000 {
		t.Errorf("expected merged bytes 3000, got %d", row.TotalBytes)
	}
	if row.TierCount(tiers.Standard) != 20 { // 5 + 15
		t.Errorf("expected Standard tier count 20, got %d", row.TierCount(tiers.Standard))
	}
	if row.TierCount(tiers.GlacierFR) != 5 {
		t.Errorf("expected GlacierFR tier count 5, got %d", row.TierCount(tiers.GlacierFR))
	}
	if row.TierCount(tiers.GlacierIR) != 5 {
		t.Errorf("expected GlacierIR tier count 5, got %d", row.TierCount(tiers.GlacierIR))
	}
	if row.TierBytes(tiers.Standard) != 2000 { // 500 + 1500
		t.Errorf("expected Standard tier bytes 2000, got %d", row.TierBytes(tiers.Standard))
	}
}

func TestParallelMerger_DefaultsFromConfig(t *testing.T) {
	def := DefaultConfig().Merge
	for _, c := range []struct {
		name string
		in   ParallelMergeConfig
		want MergeConfig
	}{
		{"zero", ParallelMergeConfig{}, def},
		{"fan_in_one", ParallelMergeConfig{NumWorkers: 3, MaxFanIn: 1}, MergeConfig{NumWorkers: 3, MaxFanIn: def.MaxFanIn}},
		{"set", ParallelMergeConfig{NumWorkers: 3, MaxFanIn: 5}, MergeConfig{NumWorkers: 3, MaxFanIn: 5}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := NewParallelMerger(c.in).config
			if got := (MergeConfig{NumWorkers: cfg.NumWorkers, MaxFanIn: cfg.MaxFanIn}); got != c.want {
				t.Errorf("merger config %+v, want %+v", got, c.want)
			}
			if cfg.BufferSize != mergeReaderBufferSize {
				t.Errorf("buffer size %d, want %d", cfg.BufferSize, mergeReaderBufferSize)
			}
		})
	}
}
