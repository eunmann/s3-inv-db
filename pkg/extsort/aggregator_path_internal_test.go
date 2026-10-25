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
