package format

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/relab/bbhash"
)

// This file collects alternative implementations of the MPHF
// hash-position computation and the parallel fingerprint pipeline
// that production no longer uses. They are kept only so the
// comparative benchmarks in mphf_compute_positions_bench_internal_test.go
// and mphf_fingerprints_bench_internal_test.go can keep proving why
// the production path (computeHashPositionsParallelSort in
// mphf_streaming.go) is the right choice.
//
// If those benches are ever retired, delete this whole file.

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

// prefixChunkItem and prefixChunkReader implement chunked parallel
// reads over the temp prefix file for the alternative
// computeFingerprintsParallel implementation below. Production uses
// a single-threaded read; this variant exists for bench comparison.

type prefixChunkItem struct {
	index       int
	prefixBytes []byte
	offset      uint64
}

type prefixChunkReader struct {
	reader        io.Reader
	n             int
	chunkSize     int
	processed     int
	currentOffset uint64
	lenBuf        [4]byte
}

func newPrefixChunkReader(reader io.Reader, n, chunkSize int) *prefixChunkReader {
	return &prefixChunkReader{reader: reader, n: n, chunkSize: chunkSize}
}

func (r *prefixChunkReader) ReadChunk() ([]prefixChunkItem, error) {
	if r.processed >= r.n {
		return nil, nil
	}
	remaining := r.n - r.processed
	thisChunk := min(remaining, r.chunkSize)
	const estimatedAvgPrefixLen = 24
	chunkBuffer := make([]byte, 0, thisChunk*estimatedAvgPrefixLen)
	items := make([]prefixChunkItem, 0, thisChunk)

	for range thisChunk {
		if _, err := io.ReadFull(r.reader, r.lenBuf[:]); err != nil {
			return nil, fmt.Errorf("read prefix length at %d: %w", r.processed, err)
		}
		prefixLen := binary.LittleEndian.Uint32(r.lenBuf[:])
		start := len(chunkBuffer)
		if cap(chunkBuffer)-start < int(prefixLen) {
			newCap := cap(chunkBuffer)*2 + int(prefixLen)
			newBuf := make([]byte, start, newCap)
			copy(newBuf, chunkBuffer)
			chunkBuffer = newBuf
		}
		chunkBuffer = chunkBuffer[:start+int(prefixLen)]
		if _, err := io.ReadFull(r.reader, chunkBuffer[start:]); err != nil {
			return nil, fmt.Errorf("read prefix at %d: %w", r.processed, err)
		}
		items = append(items, prefixChunkItem{
			index:       r.processed,
			prefixBytes: chunkBuffer[start : start+int(prefixLen)],
			offset:      r.currentOffset,
		})
		r.currentOffset += uint64(4 + prefixLen)
		r.processed++
	}

	return items, nil
}

// computeFingerprintsParallel is the alternative parallel-pipeline
// fingerprint computation kept for bench comparison only.
func (b *StreamingMPHFBuilder) computeFingerprintsParallel(
	reader *bufio.Reader,
	mph *bbhash.BBHash2,
	n int,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
) error {
	numWorkers := max(runtime.NumCPU(), 1)
	const chunkSize = 50000
	workChan := make(chan []prefixChunkItem, numWorkers*2)
	errChan := make(chan error, numWorkers)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			b.fingerprintWorker(workChan, errChan, mph, fingerprints, preorderPositions, orderedPrefixOffsets)
		})
	}

	chunkReader := newPrefixChunkReader(reader, n, chunkSize)
	if err := b.dispatchChunks(chunkReader, workChan, errChan, &wg); err != nil {
		return err
	}

	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

func (b *StreamingMPHFBuilder) fingerprintWorker(
	workChan <-chan []prefixChunkItem,
	errChan chan<- error,
	mph *bbhash.BBHash2,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
) {
	for items := range workChan {
		for _, item := range items {
			keyHash := hashBytes(item.prefixBytes)
			hashVal := mph.Find(keyHash)
			if hashVal == 0 {
				select {
				case errChan <- fmt.Errorf("%w at index %d", ErrMPHFLookupFailed, item.index):
				default:
				}

				return
			}
			hashPos := int(hashVal - 1)
			fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)
			preorderPositions[hashPos] = b.preorderPos.Slice()[item.index]
			orderedPrefixOffsets[hashPos] = item.offset
		}
	}
}

func (b *StreamingMPHFBuilder) dispatchChunks(
	chunkReader *prefixChunkReader,
	workChan chan<- []prefixChunkItem,
	errChan <-chan error,
	wg *sync.WaitGroup,
) error {
	defer func() {
		close(workChan)
		wg.Wait()
	}()
	for {
		items, err := chunkReader.ReadChunk()
		if err != nil {
			return err
		}
		if items == nil {
			return nil
		}
		workChan <- items
		select {
		case err := <-errChan:
			return err
		default:
		}
	}
}
