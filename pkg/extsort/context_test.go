package extsort

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestIndexBuilderContextCancellation verifies that AddAllWithContext
// respects context cancellation.
func TestIndexBuilderContextCancellation(t *testing.T) {
	t.Run("immediate_cancellation", func(t *testing.T) {
		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		dir := t.TempDir()
		builder, err := NewIndexBuilder(dir, "")
		if err != nil {
			t.Fatalf("NewIndexBuilder: %v", err)
		}

		// Create a run file with many rows
		runFile := filepath.Join(dir, "run_0000.bin")
		writer, err := NewRunFileWriter(runFile, 4096)
		if err != nil {
			t.Fatalf("NewRunFileWriter: %v", err)
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

		err = builder.AddAllWithContext(ctx, iter)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("timeout_cancellation", func(t *testing.T) {
		// Use a very short timeout - shorter than it would take to process all rows
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Give timeout a chance to fire
		time.Sleep(1 * time.Millisecond)

		dir := t.TempDir()

		// Create a run file with many rows
		runFile := filepath.Join(dir, "run_0000.bin")
		writer, err := NewRunFileWriter(runFile, 4096)
		if err != nil {
			t.Fatalf("NewRunFileWriter: %v", err)
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

		err = builder.AddAllWithContext(ctx, iter)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})
}

// TestAggregationContextCancellation tests that the aggregation loop
// handles context cancellation properly when the context is already cancelled.
func TestAggregationContextCancellation(t *testing.T) {
	// Create an already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Create channels for the aggregation loop pattern
	results := make(chan objectBatch, 10)

	// Send a few batches
	go func() {
		for range 5 {
			results <- objectBatch{objects: []objectRecord{
				{key: "test/obj", size: 100, tierID: 0},
			}}
		}
		close(results)
	}()

	// Run aggregation loop (similar to pipeline code)
	agg := NewAggregator(100, 0)
	var cancelled bool
	for batch := range results {
		// Check for context cancellation (same pattern as pipeline)
		select {
		case <-ctx.Done():
			cancelled = true
			// Drain remaining
			for range results {
			}
			break
		default:
		}
		if cancelled {
			break
		}
		for _, obj := range batch.objects {
			agg.AddObject(obj.key, obj.size, obj.tierID)
		}
	}

	if !cancelled {
		t.Error("expected loop to be cancelled by pre-cancelled context")
	}

	// Should have processed zero or very few objects
	if agg.ObjectCount() > 5 {
		t.Errorf("processed too many objects: %d (expected <= 5)", agg.ObjectCount())
	}
}
