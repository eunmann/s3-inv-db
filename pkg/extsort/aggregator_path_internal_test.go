package extsort

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	gocmp "github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// referenceAggregate sums every object into each of its prefix
// ancestors directly, one map lookup per level.
func referenceAggregate(keys []string, maxDepth int) map[string]PrefixRow {
	out := map[string]PrefixRow{}
	add := func(prefix string, depth uint16, size uint64, tier tiers.ID) {
		r := out[prefix]
		r.Prefix, r.Depth = prefix, depth
		r.add(size, tier)
		out[prefix] = r
	}
	for i, k := range keys {
		size, tier := uint64(len(k)+i), tiers.ID(i%3)
		add("", 0, size, tier)
		depth := uint16(1)
		for j := range len(k) {
			if k[j] != '/' {
				continue
			}
			if maxDepth > 0 && int(depth) > maxDepth {
				break
			}
			add(k[:j+1], depth, size, tier)
			depth++
		}
	}

	return out
}

func pathTestKeys(rng *rand.Rand, n int) []string {
	segs := []string{"a", "b", "ab", "a-b", "", "x.y", "a/b"}
	keys := make([]string, 0, n+6)
	keys = append(keys, "", "/", "//", "noslash", "a/", "a//b")
	for range n {
		var sb strings.Builder
		for range rng.IntN(6) {
			sb.WriteString(segs[rng.IntN(len(segs))])
			sb.WriteByte('/')
		}
		if rng.IntN(4) > 0 {
			sb.WriteString("file")
		}
		keys = append(keys, sb.String())
	}

	return keys
}

// TestAggregatorPathCacheIsExact: the rows equal a direct per-ancestor
// count for keys in random and in sorted order, at several maxDepths,
// and across Drains (the cached path never outlives the rows it holds).
func TestAggregatorPathCacheIsExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 5))
	keys := pathTestKeys(rng, 3000)
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	for _, maxDepth := range []int{0, 1, 2, 4} {
		for name, order := range map[string][]string{"random": keys, "sorted": sorted} {
			want := referenceAggregate(order, maxDepth)
			agg := NewAggregator(0, maxDepth)
			got := map[string]PrefixRow{}
			for i, k := range order {
				agg.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
				if i%700 == 699 { // flush mid-stream, as a spill does
					mergeRows(got, agg.Drain())
				}
			}
			mergeRows(got, agg.Drain())
			if diff := gocmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("maxDepth=%d %s (-reference +aggregator):\n%s", maxDepth, name, diff)
			}
		}
	}
}

// mergeRows folds drained rows into acc, as the external merge does.
func mergeRows(acc map[string]PrefixRow, rows []*PrefixRow) {
	slices.SortFunc(rows, func(a, b *PrefixRow) int { return cmp.Compare(a.Prefix, b.Prefix) })
	for _, r := range rows {
		cur, ok := acc[r.Prefix]
		if !ok {
			acc[r.Prefix] = *r

			continue
		}
		cur.Merge(r)
		acc[r.Prefix] = cur
	}
}

// TestAggregatorSegmentsAreExact: keys fed in segments (StartSegment
// per chunk) give the same totals as a direct count, and every
// DrainSorted run is strictly increasing, whether the segments are in
// order, overlap, are out of order inside, follow keys added before
// any segment, or are cut by mid-stream Drains.
func TestAggregatorSegmentsAreExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(6, 7))
	keys := pathTestKeys(rng, 4000)
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	chunk := func(ks []string, n int) [][]string {
		out := make([][]string, 0, n)
		for i := range n {
			out = append(out, ks[i*len(ks)/n:(i+1)*len(ks)/n])
		}

		return out
	}
	sortEach := func(chunks [][]string) [][]string {
		for _, c := range chunks {
			slices.Sort(c)
		}

		return chunks
	}
	inOrder := chunk(slices.Clone(sorted), 5)
	shuffled := slices.Clone(inOrder)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	oneSwap := chunk(slices.Clone(sorted), 3)
	oneSwap[1][10], oneSwap[1][900] = oneSwap[1][900], oneSwap[1][10]
	cases := map[string]struct {
		before []string // added before the first segment
		chunks [][]string
	}{
		"in_order":             {chunks: inOrder},
		"chunks_shuffled":      {chunks: shuffled},
		"overlapping_sorted":   {chunks: sortEach(chunk(slices.Clone(keys), 4))},
		"random":               {chunks: chunk(slices.Clone(keys), 4)},
		"one_key_out_of_order": {chunks: oneSwap},
		"keys_before_segments": {before: keys[:500], chunks: sortEach(chunk(slices.Clone(keys[500:]), 3))},
	}
	for name, tc := range cases {
		for _, maxDepth := range []int{0, 2} {
			for _, drainEvery := range []int{0, 333} {
				var all []string
				agg := NewAggregator(0, maxDepth)
				got := map[string]PrefixRow{}
				drain := func() {
					rows := agg.DrainSorted()
					for i := 1; i < len(rows); i++ {
						if rows[i-1].Prefix >= rows[i].Prefix {
							t.Fatalf("%s: drained %q then %q", name, rows[i-1].Prefix, rows[i].Prefix)
						}
					}
					mergeRows(got, rows)
				}
				add := func(k string) {
					i := len(all)
					all = append(all, k)
					agg.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
					if drainEvery > 0 && len(all)%drainEvery == 0 {
						drain()
					}
				}
				for _, k := range tc.before {
					add(k)
				}
				for _, c := range tc.chunks {
					agg.StartSegment()
					for _, k := range c {
						add(k)
					}
				}
				drain()
				if diff := gocmp.Diff(referenceAggregate(all, maxDepth), got, cmpopts.EquateEmpty()); diff != "" {
					t.Fatalf("%s maxDepth=%d drainEvery=%d (-reference +aggregator):\n%s", name, maxDepth, drainEvery, diff)
				}
			}
		}
	}
}
