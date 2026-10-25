package inventory

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestFirstSorted_MatchesFullSort: for any k, firstSorted returns what a
// full sort's first k elements are, on inputs with many equal keys
// broken by a unique second key.
func TestFirstSorted_MatchesFullSort(t *testing.T) {
	type item struct{ key, id int }
	byKey := func(a, b item) int {
		if c := cmp.Compare(a.key, b.key); c != 0 {
			return c
		}

		return cmp.Compare(a.id, b.id)
	}
	r := rand.New(rand.NewPCG(3, 5))
	for _, n := range []int{0, 1, 2, 7, 64, 1000} {
		for _, k := range []int{0, 1, 2, 5, n / 8, n / 2, n - 1, n, n + 3} {
			if k < 0 {
				continue
			}
			items := make([]item, n)
			for i := range items {
				items[i] = item{key: r.IntN(10), id: i}
			}
			r.Shuffle(n, func(i, j int) { items[i], items[j] = items[j], items[i] })
			want := slices.Clone(items)
			slices.SortFunc(want, byKey)
			want = want[:min(k, n)]
			if got := firstSorted(items, k, byKey); !slices.Equal(got, want) {
				t.Fatalf("n=%d k=%d: got %v, want %v", n, k, got, want)
			}
		}
	}
}
