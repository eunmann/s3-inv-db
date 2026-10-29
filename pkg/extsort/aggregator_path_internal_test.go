package extsort

import (
	"cmp"
	"maps"
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
// Drain is strictly increasing, whether the segments are in
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
					rows := agg.Drain()
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

// checkAggregatorAccounting recomputes from the nodes what the
// aggregator has counted: the estimate (every node and its key, the
// prefix of each live one, every map entry) and the prefix bytes Drain
// writes. It also checks that every map key names its node's parent and
// segment.
func checkAggregatorAccounting(t *testing.T, agg *Aggregator) {
	t.Helper()
	var keyBytes, prefixBytes int
	for i := range agg.used {
		n := agg.node(uint32(i))
		if n.row.Prefix != "" {
			keyBytes += nodeKeyHeader + len(n.row.Prefix)
		}
		if n.parent == deadNode {
			continue
		}
		for p := n; p.parent != noNode; p = agg.node(p.parent) {
			prefixBytes += len(p.row.Prefix)
		}
	}
	for key, idx := range agg.children {
		n := agg.node(idx)
		if want := string(appendNodeKey(nil, n.parent, n.row.Prefix)); key != want {
			t.Fatalf("map key %q names node %d, whose key is %q", key, idx, want)
		}
	}
	want := int64(agg.used)*aggregatorNodeBytes + int64(keyBytes+prefixBytes) +
		int64(len(agg.children))*aggregatorMapEntryBytes
	if agg.prefixBytes != prefixBytes || agg.bytes != want {
		t.Fatalf("counted %d prefix bytes and an estimate of %d; the nodes have %d and %d",
			agg.prefixBytes, agg.bytes, prefixBytes, want)
	}
}

// propertyTestKeys draws keys from segments that stress prefix order:
// empty ones ("//"), bytes below '/' ('-', '.', ' ', 0x00), a segment
// that is a byte prefix of its siblings ("a" beside "a-b" and "ab"),
// multi-byte UTF-8, invalid UTF-8, keys without a slash and the empty
// key.
func propertyTestKeys(rng *rand.Rand, n int) []string {
	segs := []string{"", "a", "a-b", "a.b", "ab", "a b", "a\x00", "\x00", "-", "é", "日本", "z\xff", "a/b"}
	keys := make([]string, 0, n)
	for range n {
		var sb strings.Builder
		for range rng.IntN(7) {
			sb.WriteString(segs[rng.IntN(len(segs))])
			if rng.IntN(8) > 0 {
				sb.WriteByte('/')
			}
		}
		keys = append(keys, sb.String())
	}

	return keys
}

// TestAggregatorDrainProperty: for random keys in random, sorted and
// per-chunk sorted order, with and without segments, at several
// maxDepths and with Drains mid-stream, every drain is strictly
// increasing and the drains add up to a direct count.
func TestAggregatorDrainProperty(t *testing.T) {
	for seed := range uint64(6) {
		rng := rand.New(rand.NewPCG(seed, 11))
		keys := propertyTestKeys(rng, 1200)
		sorted := slices.Clone(keys)
		slices.Sort(sorted)
		chunkSorted := slices.Clone(keys)
		for i := 0; i < len(chunkSorted); i += 200 {
			slices.Sort(chunkSorted[i:min(i+200, len(chunkSorted))])
		}
		orders := map[string][]string{"random": keys, "sorted": sorted, "chunk_sorted": chunkSorted}
		for name, order := range orders {
			for _, segmentEvery := range []int{0, 200, 37} {
				for _, maxDepth := range []int{0, 1, 3} {
					for _, drainEvery := range []int{0, 97, 701} {
						agg := NewAggregator(0, maxDepth)
						got := map[string]PrefixRow{}
						drain := func() {
							checkAggregatorAccounting(t, agg)
							rows := agg.Drain()
							for i := 1; i < len(rows); i++ {
								if rows[i-1].Prefix >= rows[i].Prefix {
									t.Fatalf("seed=%d %s: drained %q then %q", seed, name, rows[i-1].Prefix, rows[i].Prefix)
								}
							}
							mergeRows(got, rows)
						}
						for i, k := range order {
							if segmentEvery > 0 && i%segmentEvery == 0 {
								agg.StartSegment()
							}
							agg.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
							if drainEvery > 0 && i%drainEvery == drainEvery-1 {
								drain()
							}
						}
						drain()
						if want := referenceAggregate(order, maxDepth); !maps.Equal(want, got) {
							t.Fatalf("seed=%d %s segmentEvery=%d maxDepth=%d drainEvery=%d (-reference +aggregator):\n%s",
								seed, name, segmentEvery, maxDepth, drainEvery, gocmp.Diff(want, got))
						}
					}
				}
			}
		}
	}
}
