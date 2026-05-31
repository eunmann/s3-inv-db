package format

import (
	"fmt"
	"sync"

	"github.com/relab/bbhash"
)

// This file collects alternative implementations of the MPHF
// hash-position computation that production no longer uses. They are
// kept only so the comparative benchmarks in
// mphf_compute_positions_bench_internal_test.go can keep proving why
// the production path (computeHashPositionsParallelSort in
// mphf_streaming.go) is the right choice.
//
// If that bench is ever retired, delete this whole file.

// computeHashPositionsReverseMap is the reference implementation
// using bbhash's ReverseMap. ~17× faster than per-key Find() but
// uses a serial map build + serial lookup loop.
func (b *StreamingMPHFBuilder) computeHashPositionsReverseMap(mph *bbhash.BBHash2, n int) ([]int, error) {
	hashToOrigIdx := make(map[uint64]int, n)
	for i, h := range b.hashes.Slice() {
		hashToOrigIdx[h] = i
	}

	hashPositions := make([]int, n)
	for mphPos := uint64(1); mphPos <= uint64(n); mphPos++ {
		key := mph.Key(mphPos)
		if key == 0 {
			return nil, fmt.Errorf("%w: Key(%d) returned 0", ErrMPHFAmbiguousKey, mphPos)
		}
		origIdx, ok := hashToOrigIdx[key]
		if !ok {
			return nil, fmt.Errorf("%w: Key(%d) returned %d", ErrMPHFUnknownHash, mphPos, key)
		}
		hashPositions[origIdx] = int(mphPos - 1)
	}

	return hashPositions, nil
}

// computeHashPositionsParallelMap parallelises the reverse-map
// lookup loop across NumCPU workers. The map is built once
// serially and then read concurrently (race-free under Go's memory
// model because no goroutine writes after construction).
func (b *StreamingMPHFBuilder) computeHashPositionsParallelMap(mph *bbhash.BBHash2, n int) ([]int, error) {
	hashToOrigIdx := make(map[uint64]int, n)
	for i, h := range b.hashes.Slice() {
		hashToOrigIdx[h] = i
	}

	hashPositions := make([]int, n)
	numWorkers := mphfWorkerCount(n)

	chunkSize := (n + numWorkers - 1) / numWorkers
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		start := uint64(w*chunkSize) + 1
		end := min(uint64((w+1)*chunkSize)+1, uint64(n)+1)
		if start >= end {
			continue
		}
		wg.Go(func() {
			for mphPos := start; mphPos < end; mphPos++ {
				key := mph.Key(mphPos)
				if key == 0 {
					errs[w] = fmt.Errorf("%w: Key(%d) returned 0", ErrMPHFAmbiguousKey, mphPos)

					return
				}
				origIdx, ok := hashToOrigIdx[key]
				if !ok {
					errs[w] = fmt.Errorf("%w: Key(%d) returned %d", ErrMPHFUnknownHash, mphPos, key)

					return
				}
				hashPositions[origIdx] = int(mphPos - 1)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return hashPositions, nil
}
