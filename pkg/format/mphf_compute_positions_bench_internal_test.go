package format

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/relab/bbhash"
)

// BenchmarkComputeHashPositions compares the three computeHashPositions
// variants at realistic prefix scales. The function is the post-bbhash
// position-mapping step in StreamingMPHFBuilder.Build; for billion-row
// inventories it runs on a few tens of millions of unique prefixes.
//
// Variants:
//   - reverseMap: serial baseline (map build + serial lookup loop).
//   - parallelMap: same map, NumCPU workers read concurrently.
//   - parallelSort: sorted (hash, idx) array + parallel binary search.
//
// Run via:
//
//	go test -bench=BenchmarkComputeHashPositions -benchtime=1x \
//	    -count=10 -run=^$ ./pkg/format/...
//
// Or with the long-bench gate set for the 10M case (it allocates
// ~hundreds of MB and adds a couple of seconds per iteration):
//
//	S3INV_LONG_BENCH=1 go test -bench=BenchmarkComputeHashPositions \
//	    -benchtime=1x -count=5 -run=^$ ./pkg/format/...
func BenchmarkComputeHashPositions(b *testing.B) {
	// Sizes chosen to bracket where the map exceeds L3 cache (~32 MiB
	// per channel on the dev box): 100K stays in L3, 1M definitely
	// spills, 10M is the production-realistic ceiling.
	smallSizes := []int{100_000, 1_000_000}
	longSizes := []int{10_000_000, 50_000_000}

	for _, n := range smallSizes {
		runVariants(b, n)
	}
	if benchutil.LongBenchEnabled() {
		for _, n := range longSizes {
			runVariants(b, n)
		}
	}
}

func runVariants(b *testing.B, n int) {
	b.Helper()
	builder, mph := makeBuilderAndMPH(b, n)
	defer func() { _ = builder.Close() }()

	b.Run(fmt.Sprintf("n=%d/reverseMap", n), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := builder.computeHashPositionsReverseMap(mph, n); err != nil {
				b.Fatalf("reverseMap: %v", err)
			}
		}
	})

	b.Run(fmt.Sprintf("n=%d/parallelMap", n), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := builder.computeHashPositionsParallelMap(mph, n); err != nil {
				b.Fatalf("parallelMap: %v", err)
			}
		}
	})

	b.Run(fmt.Sprintf("n=%d/parallelSort", n), func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := builder.computeHashPositionsParallelSort(mph, n); err != nil {
				b.Fatalf("parallelSort: %v", err)
			}
		}
	})
}

// makeBuilderAndMPH constructs a StreamingMPHFBuilder populated with n
// synthetic path-shaped prefixes and the bbhash that indexes them. The
// hash and prefix counts the rest of the build pipeline relies on are
// fully realistic — this is the actual code path Build() exercises.
func makeBuilderAndMPH(b *testing.B, n int) (*StreamingMPHFBuilder, *bbhash.BBHash2) {
	b.Helper()
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir, false)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}

	// Path-shaped prefixes mirror realistic inventory keys ("a/b/c/...")
	// so per-prefix length and entropy stay representative.
	for i := range n {
		prefix := syntheticPrefix(i)
		if err := builder.Add(prefix, uint64(i)); err != nil {
			b.Fatalf("Add(%d): %v", i, err)
		}
	}

	const bbhashGamma = 2.0
	if err := builder.hashes.Freeze(); err != nil {
		b.Fatalf("freeze hashes: %v", err)
	}
	if err := builder.preorderPos.Freeze(); err != nil {
		b.Fatalf("freeze preorderPos: %v", err)
	}
	if err := builder.fingerprints.Freeze(); err != nil {
		b.Fatalf("freeze fingerprints: %v", err)
	}
	mph, err := bbhash.New(builder.hashes.Slice(), bbhash.Gamma(bbhashGamma), bbhash.WithReverseMap())
	if err != nil {
		b.Fatalf("bbhash.New: %v", err)
	}

	return builder, mph
}

// syntheticPrefix generates a "bucket-shape" prefix at the given index.
// Three levels of hierarchy mirror typical S3 layouts.
func syntheticPrefix(i int) string {
	const fanout = 64
	a := i / (fanout * fanout) % fanout
	b := i / fanout % fanout
	c := i % fanout
	return strconv.Itoa(a) + "/" + strconv.Itoa(b) + "/" + strconv.Itoa(c) + "/" + strconv.Itoa(i)
}
