package indexread_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// queryBenchSizes is the parameterised set of index sizes the
// query-path benches sweep. Defaults span the regimes where mmap
// and CPU-cache behaviour change shape; overridden by
// S3INV_BENCH_SIZES (comma-separated decimal list of object counts).
// 10M is gated behind S3INV_LONG_BENCH because the build fixture
// takes ~10s and the lookup loop ~30s.
func queryBenchSizes() []int {
	if env := os.Getenv("S3INV_BENCH_SIZES"); env != "" {
		return parseSizes(env)
	}
	sizes := []int{100_000, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 10_000_000)
	}
	return sizes
}

func parseSizes(env string) []int {
	parts := strings.Split(env, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	return out
}

// resolvePositions converts a slice of prefix strings to their
// preorder positions, dropping any that don't resolve. Used by
// position-based query benches so each iteration of the timed loop
// pays only the field-fetch cost, not the Lookup cost.
func resolvePositions(b *testing.B, idx *indexread.Index, prefixes []string) []uint64 {
	b.Helper()
	out := make([]uint64, 0, len(prefixes))
	for _, p := range prefixes {
		pos, ok := idx.Lookup(p)
		if !ok {
			continue
		}
		out = append(out, pos)
	}
	return out
}

// indexSetup holds a built index for benchmarking.
type indexSetup struct {
	IndexDir    string
	PrefixCount uint64
}

// setupIndexFromKeys creates an index from raw keys.
func setupIndexFromKeys(tb testing.TB, keys []string) *indexSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	indexDir := filepath.Join(tmpDir, "index")

	// Use extsort aggregator
	agg := extsort.NewAggregator(len(keys), 0)

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100)
		agg.AddObject(key, size, tiers.Standard)
	}

	// Drain and sort
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	// Build index
	builder, err := extsort.NewIndexBuilder(indexDir, "")
	if err != nil {
		tb.Fatalf("NewIndexBuilder failed: %v", err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		tb.Fatalf("SetPresentTiers failed: %v", err)
	}

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			tb.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		tb.Fatalf("Finalize failed: %v", err)
	}

	return &indexSetup{
		IndexDir:    indexDir,
		PrefixCount: builder.Count(),
	}
}
