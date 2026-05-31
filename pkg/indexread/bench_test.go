package indexread_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// Indexread benchmark surface lives in three files:
//   - bench_test.go       (this) — one-off probes for opens and the
//                                  subtree-size axis.
//   - grid_bench_test.go         — warm/cold query sweep across the
//                                  full shape × tier × size grid.
//   - cold_cache_bench_test.go   — cold-cache Open + concurrent-worker
//                                  scaling.

// benchIndex holds a pre-built index and its prefix list for warm
// query benchmarks.
type benchIndex struct {
	idx      *indexread.Index
	prefixes []string
	dir      string
}

// setupBenchIndex builds a fixture index from keys and opens it.
func setupBenchIndex(b *testing.B, keys []string) *benchIndex {
	b.Helper()

	setup := setupIndexFromKeys(b, keys)

	idx, err := indexread.Open(setup.IndexDir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	count := idx.Count()
	prefixes := make([]string, 0, count)
	for i := range count {
		p, err := idx.PrefixString(i)
		if err != nil {
			b.Fatalf("PrefixString failed: %v", err)
		}
		prefixes = append(prefixes, p)
	}

	return &benchIndex{
		idx:      idx,
		prefixes: prefixes,
		dir:      setup.IndexDir,
	}
}

func (bi *benchIndex) Close() {
	if bi.idx != nil {
		bi.idx.Close()
	}
}

// BenchmarkIndexOpen measures warm-cache Open latency on a small
// fixture. For the cold-cache equivalent see
// BenchmarkIndexOpen_ColdCache in cold_cache_bench_test.go.
func BenchmarkIndexOpen(b *testing.B) {
	const fixtureSize = 50_000
	tmpDir := b.TempDir()
	agg := extsort.NewAggregator(fixtureSize, 0)
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(fixtureSize))
	gen.Stream(func(o benchutil.FakeObject) {
		agg.AddObject(o.Key, o.Size, o.TierID)
	})
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilder(tmpDir, "")
	if err != nil {
		b.Fatalf("NewIndexBuilder: %v", err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		b.Fatalf("Finalize: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		idx, err := indexread.Open(tmpDir)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		idx.Close()
	}
}

// BenchmarkDescendantsSubtree measures descendants queries against
// subtrees of differing sizes — the only meaningful axis the warm
// DescendantsAtDepth benchmark has (depth-from-root with fixed N
// just measures len(children) iteration). Other call-shape coverage
// lives in BenchmarkGridQuery.
func BenchmarkDescendantsSubtree(b *testing.B) {
	const fixtureSize = 50_000
	keys := benchutil.GenerateKeys(fixtureSize, "s3_realistic")
	bi := setupBenchIndex(b, keys)
	defer bi.Close()

	var smallSubtreePrefix, largeSubtreePrefix string
	var smallCount, largeCount int

	for _, p := range bi.prefixes {
		pos, ok := bi.idx.Lookup(p)
		if !ok {
			continue
		}
		subtreeSize := bi.idx.SubtreeEnd(pos) - pos
		if subtreeSize > 5 && subtreeSize < 50 && smallSubtreePrefix == "" {
			smallSubtreePrefix = p
			smallCount = int(subtreeSize)
		}
		if subtreeSize > 500 && largeSubtreePrefix == "" {
			largeSubtreePrefix = p
			largeCount = int(subtreeSize)
		}
		if smallSubtreePrefix != "" && largeSubtreePrefix != "" {
			break
		}
	}

	b.Run("small_subtree", func(b *testing.B) {
		if smallSubtreePrefix == "" {
			b.Skip("no suitable small subtree found")
		}
		pos, _ := bi.idx.Lookup(smallSubtreePrefix)
		b.Logf("prefix=%q subtree_size=%d", smallSubtreePrefix, smallCount)
		b.ResetTimer()
		for range b.N {
			_, _ = bi.idx.DescendantsAtDepth(pos, 1)
		}
	})

	b.Run("large_subtree", func(b *testing.B) {
		if largeSubtreePrefix == "" {
			b.Skip("no suitable large subtree found")
		}
		pos, _ := bi.idx.Lookup(largeSubtreePrefix)
		b.Logf("prefix=%q subtree_size=%d", largeSubtreePrefix, largeCount)
		b.ResetTimer()
		for range b.N {
			_, _ = bi.idx.DescendantsAtDepth(pos, 1)
		}
	})
}
