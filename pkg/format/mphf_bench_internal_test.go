package format

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

// generateRealisticPrefixes generates prefixes similar to real S3 paths
// (hierarchical "bucket/dir1/dir2/file" with mixed depth).
func generateRealisticPrefixes(n int) []string {
	prefixes := make([]string, n)
	for i := range n {
		depth := 1 + (i % 5)
		var sb strings.Builder
		// The segments repeat every million; a leading block segment
		// keeps larger sets unique and leaves the first million as is.
		if block := i / 1_000_000; block > 0 {
			fmt.Fprintf(&sb, "r%d/", block)
		}
		for d := range depth {
			segmentNum := (i*7 + d*13) % 1000000
			fmt.Fprintf(&sb, "seg%d/", segmentNum)
		}
		prefixes[i] = sb.String()
	}
	return prefixes
}

// TestGenerateRealisticPrefixesUniquePastAMillion guards the long
// benchmarks' fixtures: the segments repeat every million, so without
// the block segment prefix i and i+1M would be the same string.
func TestGenerateRealisticPrefixesUniquePastAMillion(t *testing.T) {
	const tail = 10
	prefixes := generateRealisticPrefixes(1_000_000 + tail)
	for i := range tail {
		if prefixes[i] == prefixes[1_000_000+i] {
			t.Fatalf("prefix %d repeats prefix %d: %q", 1_000_000+i, i, prefixes[i])
		}
	}
}

// buildMPHFForBench builds an on-disk MPHF over the given prefixes and
// returns its directory. Setup-only; not in any timed region.
func buildMPHFForBench(b *testing.B, prefixes []string) string {
	b.Helper()
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	for _, p := range prefixes {
		if err := builder.Add(p); err != nil {
			builder.Close()
			b.Fatalf("Add: %v", err)
		}
	}
	if err := builder.Build(dir); err != nil {
		builder.Close()
		b.Fatalf("Build: %v", err)
	}
	builder.Close()
	return dir
}

// BenchmarkMPHFBuild sweeps the full streaming MPHF build path
// (Add → Build → fingerprint compute → on-disk write). Default sizes
// stress the small-cache and L3-spill regimes; the long-bench gate
// adds a 5M point that runs ~1–2 min per iter.
func BenchmarkMPHFBuild(b *testing.B) {
	sizes := []int{100_000, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 5_000_000)
	}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			prefixes := generateRealisticPrefixes(n)
			b.ResetTimer()
			b.ReportAllocs()
			var peak uint64
			for range b.N {
				runtime.GC()
				sampler := benchutil.StartHeapPeakSampler()
				_ = buildMPHFForBench(b, prefixes)
				peak = max(peak, sampler.Stop())
			}
			// Peak heap over Add + Build: the MPHF finalize is the part of
			// an index build whose memory GOMEMLIMIT does not govern.
			b.ReportMetric(float64(peak), "peak_heap_B")
		})
	}
}

// BenchmarkMPHFQuery sweeps lookup latency across MPHF sizes. Setup
// (build + open) runs once per size sub-bench; only the Lookup loop
// is timed.
func BenchmarkMPHFQuery(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 5_000_000)
	}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			prefixes := generateRealisticPrefixes(n)
			dir := buildMPHFForBench(b, prefixes)
			m, err := OpenMPHF(dir)
			if err != nil {
				b.Fatalf("OpenMPHF: %v", err)
			}
			defer m.Close()

			order := make([]int, n)
			for i := range order {
				order[i] = i
			}
			rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_, _ = m.Lookup(prefixes[order[i%n]])
			}
		})
	}
}
