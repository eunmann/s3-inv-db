package format

import (
	"slices"
	"testing"

	"github.com/relab/bbhash"
)

// TestComputeHashPositions_VariantsAgree pins the three implementations
// to byte-identical output so the benchmarks aren't measuring two
// different things and the variant we eventually default to can be
// swapped in without behaviour drift.
func TestComputeHashPositions_VariantsAgree(t *testing.T) {
	for _, n := range []int{1, 2, 8, 1024, 10000} {
		t.Run(prefixesLabel(n), func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewStreamingMPHFBuilder(dir)
			if err != nil {
				t.Fatalf("NewStreamingMPHFBuilder: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })

			for i := range n {
				if err := b.Add(syntheticPrefix(i), uint64(i)); err != nil {
					t.Fatalf("Add(%d): %v", i, err)
				}
			}

			const bbhashGamma = 2.0
			mph, err := bbhash.New(b.hashes, bbhash.Gamma(bbhashGamma), bbhash.WithReverseMap())
			if err != nil {
				t.Fatalf("bbhash.New: %v", err)
			}

			base, err := b.computeHashPositionsReverseMap(mph, n)
			if err != nil {
				t.Fatalf("reverseMap: %v", err)
			}
			pmap, err := b.computeHashPositionsParallelMap(mph, n)
			if err != nil {
				t.Fatalf("parallelMap: %v", err)
			}
			psort, err := b.computeHashPositionsParallelSort(mph, n)
			if err != nil {
				t.Fatalf("parallelSort: %v", err)
			}
			if !slices.Equal(base, pmap) {
				t.Errorf("parallelMap diverged from reverseMap at n=%d", n)
			}
			if !slices.Equal(base, psort) {
				t.Errorf("parallelSort diverged from reverseMap at n=%d", n)
			}
		})
	}
}

func prefixesLabel(n int) string {
	switch n {
	case 1:
		return "n=1"
	case 2:
		return "n=2"
	case 8:
		return "n=8"
	case 1024:
		return "n=1024"
	case 10000:
		return "n=10000"
	}

	return "n=?"
}
