package format

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/relab/bbhash"
)

// ----------------------------------------------------------------------------
// FINGERPRINT PIPELINE BENCHMARKS
// These benchmarks isolate the fingerprint computation phase from BBHash
// construction to help identify optimization opportunities.
// ----------------------------------------------------------------------------

// BenchmarkFingerprintPipeline_1M benchmarks the complete fingerprint pipeline.
// This is the primary benchmark for fingerprint performance analysis.
// Runs in ~10-20s on typical hardware.
func BenchmarkFingerprintPipeline_1M(b *testing.B) {
	benchmarkFingerprintPipeline(b, 1_000_000)
}

// BenchmarkFingerprintPipeline_5M benchmarks with 5M prefixes for profiling.
func BenchmarkFingerprintPipeline_5M(b *testing.B) {
	benchmarkFingerprintPipeline(b, 5_000_000)
}

// BenchmarkFingerprintPipeline_10M is a long-running stress test.
func BenchmarkFingerprintPipeline_10M(b *testing.B) {
	benchmarkFingerprintPipeline(b, 10_000_000)
}

func benchmarkFingerprintPipeline(b *testing.B, n int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// ----------------------------------------------------------------------------
// PHASE ISOLATION BENCHMARKS
// These isolate individual phases to measure their cost.
// ----------------------------------------------------------------------------

// BenchmarkAddPhase_1M measures just the Add phase (hashing + temp file writes).
func BenchmarkAddPhase_1M(b *testing.B) {
	benchmarkAddPhase(b, 1_000_000)
}

func benchmarkAddPhase(b *testing.B, n int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		// Only Add phase, no Build
		builder.Close()
	}
}

// BenchmarkBBHashNewOnly_1M measures just bbhash.New (no fingerprint compute).
func BenchmarkBBHashNewOnly_1M(b *testing.B) {
	benchmarkBBHashNewOnly(b, 1_000_000)
}

func benchmarkBBHashNewOnly(b *testing.B, n int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(n)
	hashes := make([]uint64, n)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := bbhash.New(hashes, bbhash.Gamma(2.0))
		if err != nil {
			b.Fatalf("bbhash.New failed: %v", err)
		}
	}
}

// BenchmarkFingerprintComputeOnly_1M measures the parallel fingerprint compute.
// This isolates fingerprint computation from Add phase and BBHash construction.
func BenchmarkFingerprintComputeOnly_1M(b *testing.B) {
	benchmarkFingerprintComputeOnly(b, 1_000_000)
}

func benchmarkFingerprintComputeOnly(b *testing.B, n int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(n)

	// Pre-build the MPHF and temp file once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	// Build MPHF (we need it for Find calls)
	if err := builder.tempWriter.Flush(); err != nil {
		b.Fatalf("flush temp file: %v", err)
	}

	mph, err := bbhash.New(builder.hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	// Allocate output arrays
	fingerprints := make([]uint64, n)
	preorderPositions := make([]uint64, n)
	orderedPrefixOffsets := make([]uint64, n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Reset arrays
		clear(fingerprints)
		clear(preorderPositions)
		clear(orderedPrefixOffsets)

		// Seek to start
		if _, err := builder.tempFile.Seek(0, 0); err != nil {
			b.Fatalf("seek temp file: %v", err)
		}
		reader := bufio.NewReaderSize(builder.tempFile, 1024*1024)

		if err := builder.computeFingerprintsParallel(
			reader, mph, n, fingerprints, preorderPositions, orderedPrefixOffsets,
		); err != nil {
			b.Fatalf("computeFingerprintsParallel failed: %v", err)
		}
	}

	builder.Close()
}

// ----------------------------------------------------------------------------
// FINGERPRINT EXPERIMENTS: Cost vs Benefit
// These benchmarks test alternative fingerprint strategies.
// ----------------------------------------------------------------------------

// FingerprintMode specifies the fingerprint computation strategy.
type FingerprintMode int

const (
	FingerprintFull     FingerprintMode = iota // Full 64-bit fingerprint (current)
	FingerprintNone                            // Skip fingerprint computation entirely
	Fingerprint32Bit                           // Truncated 32-bit fingerprint
	FingerprintXOR                             // XOR with hash position (minimal work)
	FingerprintZeroCopy                        // Zero-copy FNV without allocations
)

// BenchmarkFingerprintModes compares different fingerprint strategies.
func BenchmarkFingerprintModes(b *testing.B) {
	const n = 1_000_000
	prefixes := generateRealisticPrefixes(n)

	modes := []struct {
		name string
		mode FingerprintMode
	}{
		{"Full64", FingerprintFull},
		{"NoFingerprint", FingerprintNone},
		{"Truncated32", Fingerprint32Bit},
		{"XORPosition", FingerprintXOR},
		{"ZeroCopy", FingerprintZeroCopy},
	}

	for _, tc := range modes {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkFingerprintMode(b, prefixes, n, tc.mode)
		})
	}
}

func benchmarkFingerprintMode(b *testing.B, prefixes []string, n int, mode FingerprintMode) {
	b.Helper()
	// Pre-build the MPHF and temp file once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.tempWriter.Flush(); err != nil {
		b.Fatalf("flush temp file: %v", err)
	}

	mph, err := bbhash.New(builder.hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	fingerprints := make([]uint64, n)
	preorderPositions := make([]uint64, n)
	orderedPrefixOffsets := make([]uint64, n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		clear(fingerprints)
		clear(preorderPositions)
		clear(orderedPrefixOffsets)

		if _, err := builder.tempFile.Seek(0, 0); err != nil {
			b.Fatalf("seek temp file: %v", err)
		}
		reader := bufio.NewReaderSize(builder.tempFile, 1024*1024)

		if err := computeFingerprintsWithMode(
			reader, mph, n, fingerprints, preorderPositions, orderedPrefixOffsets,
			builder.preorderPos, mode,
		); err != nil {
			b.Fatalf("computeFingerprintsWithMode failed: %v", err)
		}
	}

	builder.Close()
}

// computeFingerprintsWithMode is a variant of computeFingerprintsParallel that
// supports different fingerprint computation modes for benchmarking.
func computeFingerprintsWithMode(
	reader *bufio.Reader,
	mph *bbhash.BBHash2,
	n int,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
	preorderPos []uint64,
	mode FingerprintMode,
) error {
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	const chunkSize = 50000
	workChan := make(chan []prefixChunkItem, numWorkers*2)
	errChan := make(chan error, numWorkers)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fingerprintModeWorker(workChan, errChan, mph, fingerprints, preorderPositions, orderedPrefixOffsets, preorderPos, mode)
		}()
	}

	// Read and dispatch chunks using shared helper
	chunkReader := newPrefixChunkReader(reader, n, chunkSize)
	err := dispatchChunksForBench(chunkReader, workChan, errChan, &wg)
	if err != nil {
		return err
	}

	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

// fingerprintModeWorker processes prefix chunks with a specific fingerprint mode.
func fingerprintModeWorker(
	workChan <-chan []prefixChunkItem,
	errChan chan<- error,
	mph *bbhash.BBHash2,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
	preorderPos []uint64,
	mode FingerprintMode,
) {
	for items := range workChan {
		for _, item := range items {
			keyHash := hashBytes(item.prefixBytes)
			hashVal := mph.Find(keyHash)
			if hashVal == 0 {
				select {
				case errChan <- fmt.Errorf("MPHF lookup failed for prefix at index %d", item.index):
				default:
				}
				return
			}
			hashPos := int(hashVal - 1)

			// Compute fingerprint based on mode
			var fp uint64
			switch mode {
			case FingerprintFull:
				fp = computeFingerprintBytes(item.prefixBytes)
			case FingerprintNone:
				fp = 0 // No fingerprint computation
			case Fingerprint32Bit:
				fp = computeFingerprintBytes(item.prefixBytes) & 0xFFFFFFFF
			case FingerprintXOR:
				fp = keyHash ^ uint64(hashPos)
			case FingerprintZeroCopy:
				fp = fnvZeroCopy(item.prefixBytes)
			}

			fingerprints[hashPos] = fp
			preorderPositions[hashPos] = preorderPos[item.index]
			orderedPrefixOffsets[hashPos] = item.offset
		}
	}
}

// dispatchChunksForBench reads prefix chunks and sends them to workers.
func dispatchChunksForBench(
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

// fnvZeroCopy is a zero-allocation FNV-1 hash implementation.
func fnvZeroCopy(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range b {
		hash *= prime64
		hash ^= uint64(c)
	}
	return hash
}

// ----------------------------------------------------------------------------
// HASHING MICROBENCHMARKS (Deeper Analysis)
// ----------------------------------------------------------------------------

// BenchmarkHashingStrategies compares different hashing approaches.
func BenchmarkHashingStrategies(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)
	prefixBytes := make([][]byte, len(prefixes))
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	b.Run("FNV1a_String_Alloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := fnv.New64a()
			h.Write([]byte(prefixes[i%len(prefixes)]))
			_ = h.Sum64()
		}
	})

	b.Run("FNV1a_Bytes_NoAlloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := fnv.New64a()
			h.Write(prefixBytes[i%len(prefixBytes)])
			_ = h.Sum64()
		}
	})

	b.Run("FNV1_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = fnvZeroCopy(prefixBytes[i%len(prefixBytes)])
		}
	})

	b.Run("FNV1a_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = fnv1aZeroCopy(prefixBytes[i%len(prefixBytes)])
		}
	})

	b.Run("StringToBytes_Conversion", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = []byte(prefixes[i%len(prefixes)])
		}
	})

	b.Run("UnsafeStringToBytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = unsafeStringToBytes(prefixes[i%len(prefixes)])
		}
	})
}

// fnv1aZeroCopy is a zero-allocation FNV-1a hash implementation.
func fnv1aZeroCopy(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

// unsafeStringToBytes converts a string to bytes without copying.
// WARNING: The returned slice must not be modified!
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BenchmarkMPHFFind measures just the mph.Find call overhead.
func BenchmarkMPHFFind(b *testing.B) {
	const n = 1_000_000
	prefixes := generateRealisticPrefixes(n)

	hashes := make([]uint64, n)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	mph, err := bbhash.New(hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_ = mph.Find(hashes[i%n])
	}
}

// BenchmarkHashAndFind measures the combined hash + Find overhead.
func BenchmarkHashAndFind(b *testing.B) {
	const n = 1_000_000
	prefixes := generateRealisticPrefixes(n)
	prefixBytes := make([][]byte, n)
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	hashes := make([]uint64, n)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	mph, err := bbhash.New(hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	b.Run("Hash+Find_Standard", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := hashBytes(prefixBytes[i%n])
			_ = mph.Find(h)
		}
	})

	b.Run("Hash+Find_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			h := fnv1aZeroCopy(prefixBytes[i%n])
			_ = mph.Find(h)
		}
	})
}

// ----------------------------------------------------------------------------
// COMBINED HASH + FINGERPRINT BENCHMARK
// ----------------------------------------------------------------------------

// BenchmarkPerPrefixWork measures all the per-prefix work in fingerprint phase.
func BenchmarkPerPrefixWork(b *testing.B) {
	const n = 1_000_000
	prefixes := generateRealisticPrefixes(n)
	prefixBytes := make([][]byte, n)
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	hashes := make([]uint64, n)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	mph, err := bbhash.New(hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	fingerprints := make([]uint64, n)
	positions := make([]uint64, n)

	b.Run("Full_KeyHash+Find+Fingerprint", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]

			// This is exactly what happens per prefix in computeFingerprintsParallel
			keyHash := hashBytes(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			fingerprints[hashPos] = computeFingerprintBytes(pb)
			positions[hashPos] = uint64(idx)
		}
	})

	b.Run("ZeroCopy_KeyHash+Find+Fingerprint", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]

			keyHash := fnv1aZeroCopy(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			fingerprints[hashPos] = fnvZeroCopy(pb)
			positions[hashPos] = uint64(idx)
		}
	})

	b.Run("NoFingerprint_KeyHash+Find", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			idx := i % n
			pb := prefixBytes[idx]

			keyHash := hashBytes(pb)
			hashVal := mph.Find(keyHash)
			hashPos := int(hashVal - 1)
			positions[hashPos] = uint64(idx)
			_ = hashPos
		}
	})
}

// ----------------------------------------------------------------------------
// SCALING BENCHMARKS
// ----------------------------------------------------------------------------

// BenchmarkFingerprintPipelineScaling measures how fingerprint build scales with N.
func BenchmarkFingerprintPipelineScaling(b *testing.B) {
	sizes := []int{100_000, 500_000, 1_000_000, 2_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			benchmarkFingerprintPipeline(b, size)
		})
	}
}

// BenchmarkBBHashScaling measures how BBHash construction scales with N.
func BenchmarkBBHashScaling(b *testing.B) {
	sizes := []int{100_000, 500_000, 1_000_000, 2_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			benchmarkBBHashNewOnly(b, size)
		})
	}
}

// ----------------------------------------------------------------------------
// OPTIMIZATION COMPARISON BENCHMARKS
// ----------------------------------------------------------------------------
// These benchmarks compare the optimized build approach (Options 2+4) against
// the baseline to measure the impact of:
// - Option 2: Separating Find() into a tight loop for cache efficiency
// - Option 4: Pre-computing fingerprints during Add phase

// BenchmarkOptimizedBuild_1M benchmarks the new optimized Build path.
func BenchmarkOptimizedBuild_1M(b *testing.B) {
	benchmarkOptimizedBuild(b, 1_000_000)
}

// BenchmarkOptimizedBuild_500K benchmarks the optimized Build with 500K prefixes.
func BenchmarkOptimizedBuild_500K(b *testing.B) {
	benchmarkOptimizedBuild(b, 500_000)
}

func benchmarkOptimizedBuild(b *testing.B, n int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// BenchmarkBuildPhaseBreakdown_AddPhase measures the Add phase cost.
func BenchmarkBuildPhaseBreakdown_AddPhase(b *testing.B) {
	const n = 500_000
	prefixes := generateRealisticPrefixes(n)

	b.ReportAllocs()
	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}
		builder.Close()
	}
}

// BenchmarkBuildPhaseBreakdown_BBHash measures BBHash construction cost.
func BenchmarkBuildPhaseBreakdown_BBHash(b *testing.B) {
	const n = 500_000
	prefixes := generateRealisticPrefixes(n)

	// Setup: complete Add phase once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}
	builder.tempWriter.Flush()

	// Copy hashes for repeated use
	hashes := make([]uint64, len(builder.hashes))
	copy(hashes, builder.hashes)
	builder.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := bbhash.New(hashes, bbhash.Gamma(2.0))
		if err != nil {
			b.Fatalf("bbhash.New failed: %v", err)
		}
	}
}

// BenchmarkBuildPhaseBreakdown_Find measures the Find() tight loop cost.
func BenchmarkBuildPhaseBreakdown_Find(b *testing.B) {
	const n = 500_000
	prefixes := generateRealisticPrefixes(n)

	// Setup: complete Add phase and build MPHF once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}
	builder.tempWriter.Flush()

	// Copy hashes
	hashes := make([]uint64, len(builder.hashes))
	copy(hashes, builder.hashes)

	mph, err := bbhash.New(hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}
	builder.Close()

	hashPositions := make([]int, n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Tight loop: just Find() calls
		for i, h := range hashes {
			hashVal := mph.Find(h)
			hashPositions[i] = int(hashVal - 1)
		}
	}
}

// BenchmarkBuildPhaseBreakdown_ArrayMapping measures the array copy cost.
func BenchmarkBuildPhaseBreakdown_ArrayMapping(b *testing.B) {
	const n = 500_000
	prefixes := generateRealisticPrefixes(n)

	// Setup: pre-compute hash positions
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}
	builder.tempWriter.Flush()

	mph, err := bbhash.New(builder.hashes, bbhash.Gamma(2.0))
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	hashPositions := make([]int, n)
	for i, h := range builder.hashes {
		hashPositions[i] = int(mph.Find(h) - 1)
	}

	// Keep copies of source data
	fingerprints := make([]uint64, len(builder.fingerprints))
	copy(fingerprints, builder.fingerprints)
	preorderPos := make([]uint64, len(builder.preorderPos))
	copy(preorderPos, builder.preorderPos)
	builder.Close()

	outputFP := make([]uint64, n)
	outputPos := make([]uint64, n)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Simple copy loop
		for i, hashPos := range hashPositions {
			outputFP[hashPos] = fingerprints[i]
			outputPos[hashPos] = preorderPos[i]
		}
	}
}

// BenchmarkReverseMapPositions measures the ReverseMap-based position lookup.
func BenchmarkReverseMapPositions(b *testing.B) {
	const n = 500_000
	prefixes := generateRealisticPrefixes(n)

	// Setup: complete Add phase and build MPHF with ReverseMap once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}
	builder.tempWriter.Flush()

	mph, err := bbhash.New(builder.hashes, bbhash.Gamma(2.0), bbhash.WithReverseMap())
	if err != nil {
		b.Fatalf("bbhash.New failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := builder.computeHashPositionsReverseMap(mph, n)
		if err != nil {
			b.Fatalf("computeHashPositionsReverseMap failed: %v", err)
		}
	}

	builder.Close()
}
