package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkBrowse_PerCallVsBatched isolates the pos→prefix axis of the
// Browse path. For a real depth-1 child set drawn from the index, it
// compares:
//
//   - per-call: the existing handler shape (loop over positions calling
//     idx.PrefixString each time).
//   - batched: a single idx.PrefixStrings(positions) call that opens
//     one vellum iterator (FST backend) and walks forward through it.
//
// The point is to show whether Option B's Browse regression collapses
// once the handler is rewritten in batched form.
func BenchmarkBrowse_PerCallVsBatched(b *testing.B) {
	silenceZerolog(b)
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()

			// Pick the largest depth-1 child set we can find — that's
			// what stresses the pos→prefix path in a real Browse.
			parents := generateLookupPrefixes(n)
			var positions []uint64
			for _, p := range parents {
				pos, ok := idx.Lookup(p)
				if !ok {
					continue
				}
				kids, err := idx.DescendantsAtDepth(pos, 1)
				if err != nil {
					continue
				}
				if len(kids) > len(positions) {
					positions = kids
				}
			}
			// Fall back to depth-1 children of the root if nothing
			// bigger surfaced from the random lookup set.
			if rootKids, err := idx.DescendantsAtDepth(0, 1); err == nil && len(rootKids) > len(positions) {
				positions = rootKids
			}
			if len(positions) == 0 {
				b.Skip("no depth-1 children")
			}

			b.Run(fmt.Sprintf("children=%d/per_call", len(positions)), func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for range b.N {
					for _, p := range positions {
						_, _ = idx.PrefixString(p)
					}
				}
			})
			b.Run(fmt.Sprintf("children=%d/batched", len(positions)), func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for range b.N {
					_, _ = idx.PrefixStrings(positions)
				}
			})
		})
	}
}
