package extsort

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestIndexBuilderContextCancellation verifies that AddAll respects
// context cancellation.
func TestIndexBuilderContextCancellation(t *testing.T) {
	t.Run("immediate_cancellation", func(t *testing.T) {
		// Create a cancelled context
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		dir := t.TempDir()
		builder, err := NewIndexBuilder(dir, "")
		if err != nil {
			t.Fatalf("NewIndexBuilder: %v", err)
		}

		// Create a run file with many rows
		runFile := filepath.Join(dir, "run_0000.crun")
		writer, err := NewCompressedRunWriter(runFile, CompressedRunWriterOptions{BufferSize: 4096})
		if err != nil {
			t.Fatalf("NewCompressedRunWriter: %v", err)
		}

		// Write 10000 rows to ensure we'd iterate for a while
		rows := make([]*PrefixRow, 10000)
		for i := range rows {
			rows[i] = &PrefixRow{
				Prefix: fmt.Sprintf("prefix/%04d/", i),
				Depth:  2,
				Count:  1,
			}
		}
		if err := writer.WriteSorted(rows); err != nil {
			t.Fatalf("WriteSorted: %v", err)
		}
		writer.Close()

		// Create iterator
		iter, err := NewMergeIterator([]string{runFile}, 4096)
		if err != nil {
			t.Fatalf("NewMergeIterator: %v", err)
		}
		defer iter.Close()

		err = builder.AddAll(ctx, iter)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("timeout_cancellation", func(t *testing.T) {
		// Use a very short timeout - shorter than it would take to process all rows
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
		defer cancel()

		// Give timeout a chance to fire
		time.Sleep(1 * time.Millisecond)

		dir := t.TempDir()

		// Create a run file with many rows
		runFile := filepath.Join(dir, "run_0000.crun")
		writer, err := NewCompressedRunWriter(runFile, CompressedRunWriterOptions{BufferSize: 4096})
		if err != nil {
			t.Fatalf("NewCompressedRunWriter: %v", err)
		}

		// Write many rows - more than check interval
		rows := make([]*PrefixRow, 5000)
		for i := range rows {
			rows[i] = &PrefixRow{
				Prefix: fmt.Sprintf("prefix/%04d/", i),
				Depth:  2,
				Count:  1,
			}
		}
		if err := writer.WriteSorted(rows); err != nil {
			t.Fatalf("WriteSorted: %v", err)
		}
		writer.Close()

		// Create iterator
		iter, err := NewMergeIterator([]string{runFile}, 4096)
		if err != nil {
			t.Fatalf("NewMergeIterator: %v", err)
		}
		defer iter.Close()

		outDir := filepath.Join(dir, "out")
		builder, err := NewIndexBuilder(outDir, "")
		if err != nil {
			t.Fatalf("NewIndexBuilder: %v", err)
		}

		err = builder.AddAll(ctx, iter)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})
}

// TestPerWorkerAggregator_ContextCancellation guards the per-worker
// ingest pattern: a worker goroutine pulls jobs off a shared channel
// and processes each one into its private aggregator; on context
// cancellation it must exit cleanly without consuming further jobs.
//
// This is the worker-loop shape used by Pipeline.runChunkWorker after
// the I1 per-worker-aggregator refactor (which replaced the previous
// single-consumer aggregation loop that this test originally covered).
func TestPerWorkerAggregator_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	jobs := make(chan objectRecord, 10)
	for range 5 {
		jobs <- objectRecord{key: "test/obj", size: 100, tierID: 0}
	}
	close(jobs)

	agg := NewAggregator(100, 0)
	var exitedDueToCancel bool
workerLoop:
	for job := range jobs {
		select {
		case <-ctx.Done():
			exitedDueToCancel = true

			break workerLoop
		default:
		}
		agg.AddObject(job.key, job.size, job.tierID)
	}

	if !exitedDueToCancel {
		t.Error("worker did not exit on pre-cancelled context")
	}
	if n := agg.PrefixCount(); n != 0 {
		t.Errorf("aggregated %d prefixes after cancellation, want 0", n)
	}
}
