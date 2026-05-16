package extsort //nolint:testpackage // bench uses unexported objectRecord

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
)

// BenchmarkAggregationConcurrency_SharedVsPerWorker measures the
// throughput difference between two aggregation models given N
// CPU-bound goroutines each consuming a slice of objects:
//
//   - "shared":     N producer goroutines push objectRecords through a
//     channel into ONE shared aggregator consumed by a
//     single goroutine. This is the pre-I1 pipeline model.
//   - "per_worker": N worker goroutines each own a private aggregator
//     and call AddObject directly with zero coordination
//     between them. This is the I1 model.
//
// The headline number is the per_worker variant's wall-time
// reduction vs shared at NumCPU workers. At N>>1 the shared model
// is bottlenecked at the single consumer's AddObject throughput;
// per_worker scales close to linearly because each worker has its
// own map + own GC pressure.
//
// Disk + merge are not exercised — this benchmark isolates the
// concurrency model on the aggregator hot path only.
func BenchmarkAggregationConcurrency_SharedVsPerWorker(b *testing.B) {
	const totalObjects = 500_000
	objects := benchutil.NewGenerator(benchutil.S3RealisticConfig(totalObjects)).Generate()

	for _, workers := range []int{1, 2, 4, runtime.NumCPU()} {
		shardSize := totalObjects / workers
		b.Run(fmt.Sprintf("shared_workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				runSharedAggregator(workers, shardSize, objects)
			}
		})
		b.Run(fmt.Sprintf("per_worker_workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				runPerWorkerAggregator(workers, shardSize, objects)
			}
		})
	}
}

func runSharedAggregator(workers, shardSize int, objects []benchutil.FakeObject) {
	const aggCap = 50_000
	agg := NewAggregator(aggCap, 0)
	ch := make(chan objectRecord, workers*1024)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * shardSize
			end := start + shardSize
			for i := start; i < end; i++ {
				ch <- objectRecord{key: objects[i].Key, size: objects[i].Size, tierID: objects[i].TierID}
			}
		}(w)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for obj := range ch {
		agg.AddObject(obj.key, obj.size, obj.tierID)
	}
	_ = agg.Drain()
}

func runPerWorkerAggregator(workers, shardSize int, objects []benchutil.FakeObject) {
	const aggCap = 50_000
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			agg := NewAggregator(aggCap, 0)
			start := workerID * shardSize
			end := start + shardSize
			for i := start; i < end; i++ {
				agg.AddObject(objects[i].Key, objects[i].Size, objects[i].TierID)
			}
			_ = agg.Drain()
		}(w)
	}
	wg.Wait()
}
