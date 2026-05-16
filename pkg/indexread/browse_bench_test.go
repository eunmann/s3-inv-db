package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkBrowse_Matrix is the end-to-end Browse-a-parent latency
// bench. It simulates a real handler request: take a parent prefix,
// resolve it (Lookup), enumerate its direct children
// (DescendantsAtDepth), and for each child fetch the data a UI
// would render: its prefix string + its core stats + its tier
// breakdown.
//
// This is the user's stated production-critical query #2 ("get
// the children of this prefix and all of their stats").
//
// Matrix: cache {warm, cold} × n {100K, 1M}. Reports per-call
// ns/op + B/op + allocs/op.
func BenchmarkBrowse_Matrix(b *testing.B) {
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			parents := pickBrowseParents(b, idx, 32)
			if len(parents) == 0 {
				b.Skip("no browseable parents")
			}
			b.Run("warm", func(b *testing.B) {
				runBrowseLoop(b, idx, parents)
			})
			b.Run("cold", func(b *testing.B) {
				runBrowseColdLoop(b, idx, dir, parents)
			})
		})
	}
}

func runBrowseLoop(b *testing.B, idx *indexread.Index, parents []uint64) {
	b.Helper()
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		browseOnce(idx, parents[i%len(parents)])
	}
}

func runBrowseColdLoop(b *testing.B, idx *indexread.Index, dir string, parents []uint64) {
	b.Helper()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i += coldQueryBatch {
		b.StopTimer()
		dropPageCache(b, dir)
		b.StartTimer()
		batch := coldQueryBatch
		if i+batch > b.N {
			batch = b.N - i
		}
		for j := range batch {
			browseOnce(idx, parents[(i+j)%len(parents)])
		}
	}
}

// browseOnce simulates one "show me this parent's children" handler
// call: list direct children, and for each one fetch the data a UI
// would render.
func browseOnce(idx *indexread.Index, parent uint64) {
	children, err := idx.DescendantsAtDepth(parent, 1)
	if err != nil {
		return
	}
	for _, c := range children {
		_, _ = idx.PrefixString(c)
		_ = idx.Stats(c)
		_ = idx.TierBreakdown(c)
	}
}

// pickBrowseParents returns positions that have at least one direct
// descendant (so Browse actually has work to do). Selects a mix of
// parent depths to reflect realistic UI navigation.
func pickBrowseParents(b *testing.B, idx *indexread.Index, want int) []uint64 {
	b.Helper()
	prefixes := generateLookupPrefixes(int(idx.Count()))
	out := make([]uint64, 0, want)
	for _, p := range prefixes {
		pos, ok := idx.Lookup(p)
		if !ok {
			continue
		}
		children, err := idx.DescendantsAtDepth(pos, 1)
		if err != nil || len(children) == 0 {
			continue
		}
		out = append(out, pos)
		if len(out) >= want {
			break
		}
	}

	return out
}
