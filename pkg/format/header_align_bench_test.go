package format

import (
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkArrayReaderU64 measures GetU64 throughput. With the current
// 20-byte header, u64 elements are 4-byte aligned (not 8-byte). Whether
// that hurts is measurable here.
func BenchmarkArrayReaderU64(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.u64")
	const N = 1_000_000

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		b.Fatal(err)
	}
	for i := range uint64(N) {
		if err := w.WriteU64(i * 31); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	r, err := OpenArray(path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var sink uint64
		for i := range b.N {
			sink ^= r.UnsafeGetU64(uint64(i % N))
		}
		_ = sink
	})

	b.Run("Random", func(b *testing.B) {
		// Pseudo-random indices, precomputed to avoid distorting measurement.
		idx := make([]uint64, 4096)
		x := uint64(0x9E3779B97F4A7C15)
		for i := range idx {
			x = x*6364136223846793005 + 1442695040888963407
			idx[i] = x % N
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink uint64
		for i := range b.N {
			sink ^= r.UnsafeGetU64(idx[i%len(idx)])
		}
		_ = sink
	})
}

// BenchmarkOpenMmap measures Open+Close overhead. Relevant to the
// OpenRunFileAuto double-open critique and the no-madvise critique.
func BenchmarkOpenMmap(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.bin")
	if err := os.WriteFile(path, make([]byte, 16*1024*1024), 0o600); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m, err := OpenMmap(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = m.Data()[0]
		m.Close()
	}
}
