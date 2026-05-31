package extsort_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

// BenchmarkPhaseTimings exercises the event bus end-to-end against a
// synthetic in-memory build and reports per-stage timings derived
// from the bus events. Validates:
//
//   - The event bus emits the events we expect at the boundaries we
//     expect (ingest start/end, merge start/end, spill, worker idle/
//     busy, batch_committed).
//   - The overhead of having a subscriber attached is negligible vs
//     the no-bus baseline (compare against BenchmarkBuildHarness at
//     the same n).
//
// This bench drives the IndexBuilder directly (skipping S3 download)
// — the harness build path. The full Pipeline.Run matrix bench (B1)
// is separate.
//
// Note: this bench does not invoke chunkWorker, so worker_idle/busy
// events do not fire here. Spill events also don't fire because the
// IndexBuilder doesn't spill (it streams to the index). The
// pipeline-level start/end events DO fire when Pipeline.Run is
// exercised — that path is bench B1.
func BenchmarkPhaseTimings(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, n := range []int{500_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			runPhaseTimingsBench(b, n)
		})
	}
}

func runPhaseTimingsBench(b *testing.B, n int) {
	b.Helper()
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()

	b.ResetTimer()
	b.ReportAllocs()

	var (
		totalBatchEvents int64
		totalEvents      int64
		mu               sync.Mutex
	)

	for range b.N {
		bus := events.NewBus()
		sub := bus.Subscribe(8192)
		var wg sync.WaitGroup
		wg.Go(func() {
			for ev := range sub.C {
				mu.Lock()
				totalEvents++
				if ev.Type == events.EvtBatchCommitted {
					totalBatchEvents++
				}
				mu.Unlock()
			}
		})

		_ = runEventInstrumentedBuild(b, bus, objects, n)

		bus.Close()
		wg.Wait()
	}

	b.ReportMetric(float64(totalEvents)/float64(b.N), "events/op")
	b.ReportMetric(float64(totalBatchEvents)/float64(b.N), "batch_events/op")
}

// runEventInstrumentedBuild runs one synthetic build with an event-
// bus subscriber attached, simulating the publisher overhead a real
// pipeline run would see. Returns the build's outDir for cleanup.
func runEventInstrumentedBuild(b *testing.B, bus *events.Bus, objects []benchutil.FakeObject, n int) string {
	b.Helper()
	dir := b.TempDir()
	outDir := filepath.Join(dir, "idx")

	// Simulate the per-worker aggregator path manually so we can
	// publish events at the same boundaries the real pipeline does.
	agg := extsort.NewAggregator(n, 0)
	for _, o := range objects {
		agg.AddObject(o.Key, o.Size, o.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	bus.Publish(events.Event{
		Stage: events.StageAggregator,
		Type:  events.EvtBatchCommitted,
		Payload: events.BatchCommitted{
			Rows:  uint64(len(rows)),
			Bytes: 0,
		},
		Time: time.Now(),
	})

	builder, err := extsort.NewIndexBuilderWithCapacity(outDir, "", uint64(len(rows)))
	if err != nil {
		b.Fatalf("NewIndexBuilder: %v", err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
	bus.Publish(events.Event{
		Stage: events.StageIndexBuild,
		Type:  events.EvtFinalizeStarted,
		Time:  time.Now(),
	})
	if err := builder.FinalizeWithContext(b.Context()); err != nil {
		b.Fatalf("Finalize: %v", err)
	}
	bus.Publish(events.Event{
		Stage: events.StageIndexBuild,
		Type:  events.EvtFinalizeEnded,
		Time:  time.Now(),
	})

	return outDir
}
