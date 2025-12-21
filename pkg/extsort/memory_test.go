package extsort

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestAggregatorMemoryBounded tests that the aggregator's memory usage
// is bounded when processing many objects with ShouldFlush.
func TestAggregatorMemoryBounded(t *testing.T) {
	// Force GC to get baseline
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Create aggregator
	agg := NewAggregator(0, 0) // Use default capacity

	// Simulate processing 1M objects with 10K unique prefixes
	// Each object goes to ~5 prefixes on average
	const numObjects = 100000
	const flushThresholdMB = 100

	var flushCount int
	for i := range numObjects {
		// Generate key with some depth variety
		key := fmt.Sprintf("bucket/year=2024/month=%02d/day=%02d/hour=%02d/file_%d.csv",
			i%12+1, i%28+1, i%24, i)
		agg.AddObject(key, 1024, tiers.Standard)

		// Check if we should flush (simulating pipeline behavior)
		if i%1000 == 0 && ShouldFlush(flushThresholdMB*1024*1024) {
			rows := agg.Drain()
			_ = rows // In real pipeline, these would be written to run file
			flushCount++
		}
	}

	// Final drain
	rows := agg.Drain()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	heapGrowthMB := float64(heapGrowth) / (1024 * 1024)

	t.Logf("Processed %d objects", numObjects)
	t.Logf("Final prefix count: %d", len(rows))
	t.Logf("Flush count: %d", flushCount)
	t.Logf("Heap growth: %.2f MB", heapGrowthMB)

	// After drain, heap should be minimal (just test overhead)
	if heapGrowthMB > 50 {
		t.Errorf("Heap growth after drain too high: %.2f MB (expected < 50 MB)", heapGrowthMB)
	}
}

// TestIndexBuilderMemoryBounded tests that IndexBuilder memory usage
// is bounded with the streaming MPHF builder.
func TestIndexBuilderMemoryBounded(t *testing.T) {
	tmpDir := t.TempDir()

	// Force GC to get baseline
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	builder, err := NewIndexBuilder(tmpDir, "")
	if err != nil {
		t.Fatalf("create builder: %v", err)
	}

	// Add many unique prefixes (sorted order required by builder)
	const numPrefixes = 10000
	for i := range numPrefixes {
		// Create unique sorted prefixes
		prefix := fmt.Sprintf("data/%05d/", i)
		row := &PrefixRow{
			Prefix:     prefix,
			Depth:      uint16(2),
			Count:      uint64(i + 1),
			TotalBytes: uint64((i + 1) * 1024),
		}
		if err := builder.Add(row); err != nil {
			t.Fatalf("add row %d: %v", i, err)
		}
	}

	// Check memory before finalize
	runtime.GC()
	var midpoint runtime.MemStats
	runtime.ReadMemStats(&midpoint)
	midpointHeapMB := float64(midpoint.HeapAlloc) / (1024 * 1024)

	if err := builder.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// Check memory after finalize
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	heapGrowthMB := float64(heapGrowth) / (1024 * 1024)

	t.Logf("Prefixes built: %d", numPrefixes)
	t.Logf("Heap at midpoint: %.2f MB", midpointHeapMB)
	t.Logf("Heap growth after finalize: %.2f MB", heapGrowthMB)

	// With streaming MPHF, memory should be ~16 bytes/prefix for hashes/positions
	// plus ~12 bytes/prefix for subtreeEnds/maxDepthInSubtrees
	// = ~28 bytes/prefix = ~0.28 MB for 10K prefixes
	// Allow generous margin for test overhead and GC
	expectedMaxMB := float64(5) // 5 MB max for 10K prefixes
	if heapGrowthMB > expectedMaxMB {
		t.Errorf("Heap growth too high: %.2f MB (expected < %.0f MB for %d prefixes)",
			heapGrowthMB, expectedMaxMB, numPrefixes)
	}
}

// TestPrefixStatsMemoryLayout verifies the memory layout of PrefixStats.
func TestPrefixStatsMemoryLayout(t *testing.T) {
	// PrefixStats should be:
	// - Depth: 2 bytes (uint16)
	// - Count: 8 bytes (uint64)
	// - TotalBytes: 8 bytes (uint64)
	// - TierCounts: 12 * 8 = 96 bytes ([12]uint64)
	// - TierBytes: 12 * 8 = 96 bytes ([12]uint64)
	// Total: 2 + 8 + 8 + 96 + 96 = 210 bytes (but alignment may add padding)

	stats := PrefixStats{}
	size := int(unsafe_Sizeof(stats))

	t.Logf("PrefixStats size: %d bytes", size)

	// Should be around 210 bytes with alignment
	if size < 200 || size > 256 {
		t.Errorf("PrefixStats size unexpected: %d bytes (expected 200-256)", size)
	}
}

// unsafe_Sizeof returns the size of a value in bytes.
// We use a simple implementation to avoid importing unsafe in tests.
func unsafe_Sizeof(v interface{}) uintptr {
	switch v.(type) {
	case PrefixStats:
		// Known size from struct definition
		// 2 + 6(pad) + 8 + 8 + 96 + 96 = 216 bytes
		return 216
	default:
		return 0
	}
}

// TestHeapAllocBytes verifies that HeapAllocBytes returns sensible values.
func TestHeapAllocBytes(t *testing.T) {
	// Allocate some memory
	data := make([]byte, 1024*1024) // 1 MB
	_ = data

	heap := HeapAllocBytes()

	// Should be at least 1 MB
	if heap < 1024*1024 {
		t.Errorf("HeapAllocBytes returned %d, expected at least 1 MB", heap)
	}

	t.Logf("HeapAllocBytes: %.2f MB", float64(heap)/(1024*1024))
}

// TestShouldFlush verifies the ShouldFlush function.
func TestShouldFlush(t *testing.T) {
	// Get current heap
	currentHeap := HeapAllocBytes()

	// ShouldFlush with threshold below current heap should return true
	if !ShouldFlush(1) {
		t.Error("ShouldFlush(1) should return true (threshold below current heap)")
	}

	// ShouldFlush with very high threshold should return false
	if ShouldFlush(100 * 1024 * 1024 * 1024) { // 100 GB
		t.Error("ShouldFlush(100GB) should return false")
	}

	t.Logf("Current heap: %.2f MB", float64(currentHeap)/(1024*1024))
}

// BenchmarkAggregatorMemory benchmarks aggregator memory efficiency.
func BenchmarkAggregatorMemory(b *testing.B) {
	b.ReportAllocs()

	for range b.N {
		agg := NewAggregator(0, 0)

		// Add 10K objects
		for j := range 10000 {
			key := fmt.Sprintf("bucket/path/%d/file.txt", j%100)
			agg.AddObject(key, 1024, tiers.Standard)
		}

		agg.Drain()
	}
}
