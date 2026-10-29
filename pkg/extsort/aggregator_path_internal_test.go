package extsort

import (
	"cmp"
	"encoding/binary"
	"fmt"
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
// ancestors directly, one map lookup per level, for the objects the
// tests add: key i with size len(key)+i in tier i%3.
func referenceAggregate(keys []string, maxDepth int) map[string]PrefixRow {
	return referenceAggregateTiers(keys, maxDepth, func(i int) tiers.ID { return tiers.ID(i % 3) })
}

// referenceAggregateTiers is referenceAggregate with key i in tier
// tierOf(i).
func referenceAggregateTiers(keys []string, maxDepth int, tierOf func(i int) tiers.ID) map[string]PrefixRow {
	out := map[string]PrefixRow{}
	add := func(prefix string, depth uint16, size uint64, tier tiers.ID) {
		r := out[prefix]
		r.Prefix, r.Depth = prefix, depth
		r.add(size, tier)
		out[prefix] = r
	}
	for i, k := range keys {
		size, tier := uint64(len(k)+i), tierOf(i)
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

// adversarialKeySequences are key sequences whose consecutive keys
// share a prefix ending at the edges AddObject decides order and path
// reuse by: keys that are prefixes of each other, equal keys, keys out of
// order only at their last byte, shared prefixes ending at a '/' or just
// after one, bytes either side of '/', keys without a '/' and keys
// around a maxDepth of 1 to 3.
var adversarialKeySequences = map[string][]string{
	"prefixes_ascending":  {"", "a", "a/", "a/b", "a/b/", "a/b/c", "a/b/c/", "a/b/c/d"},
	"prefixes_descending": {"a/b/c/d", "a/b/c/", "a/b/c", "a/b/", "a/b", "a/", "a", ""},
	"prefix_then_longer":  {"a/b/", "a/b/c/", "a/b/", "a/b/c/d/e/", "a/b/c/d/e/f", "a/b/c/d/e/"},
	"equal":               {"", "", "a/b/c", "a/b/c", "a/b/c", "x/", "x/", "noslash", "noslash"},
	"last_byte_down":      {"a/b/c2", "a/b/c1"},
	"last_byte_down_dir":  {"a/b/x/", "a/b/x."},
	"last_byte_down_seg":  {"a/b1/", "a/b0/"},
	"last_byte_up":        {"a/b/c1", "a/b/c2", "a/b/x.", "a/b/x/"},
	"lcp_at_slash":        {"a/b/c", "a/bc", "a/b-/x", "a/b/x", "a/b", "a/b/"},
	"lcp_after_slash":     {"a/b/c", "a/b/d", "a/b/", "a/b/x/y", "a/b/x/z/", "a/b/y/"},
	"lcp_after_slash_up":  {"a/b/", "a/b/c", "a/b/c/", "a/b/c/d", "a/b/d/", "a/c/"},
	"no_slash":            {"abc", "abd", "ab", "abc", "b", "a"},
	"no_slash_ascending":  {"", "a", "ab", "abc", "abd", "b"},
	"max_depth":           {"a/b/c/d/e", "a/b/c/d/f", "a/b/c/x/e", "a/b/y/d/e", "a/b/", "a/b/c/", "a/b/c/d/", "a/b/c/d/e/"},
	"max_depth_down":      {"a/b/c/d/f", "a/b/c/d/e", "a/b/c/e/", "a/b/c/", "a/b/c"},
	"empty_segments":      {"//", "///", "//a", "/", "a//", "a//b", "a/"},
}

// TestAggregatorAdversarialOrdersAreExact: for each adversarial
// sequence, forward and reversed, an ordered segment maps exactly at the
// first key below the one before it, and the rows equal a direct count
// whether the keys come in one segment, without segments, or in
// segments cut by Drains, at maxDepths around the keys' depths.
func TestAggregatorAdversarialOrdersAreExact(t *testing.T) {
	var all []string
	for _, seq := range adversarialKeySequences {
		all = append(all, seq...)
	}
	sortedAll := slices.Clone(all)
	slices.Sort(sortedAll)
	sequences := map[string][]string{"all": all, "all_sorted": sortedAll}
	for name, seq := range adversarialKeySequences {
		sequences[name] = seq
		reversed := slices.Clone(seq)
		slices.Reverse(reversed)
		sequences[name+"/reversed"] = reversed
	}
	for name, seq := range sequences {
		agg := NewAggregator(0, 0)
		agg.StartSegment()
		for i, k := range seq {
			agg.AddObject(k, 1, tiers.Standard)
			if want := slices.IsSorted(seq[:i+1]); agg.inOrder() != want {
				t.Fatalf("%s: after key %d (%q), in order = %v, want %v", name, i, k, agg.inOrder(), want)
			}
		}
		for _, maxDepth := range []int{0, 1, 2, 3, 4} {
			for _, segmentEvery := range []int{0, len(seq), 3} {
				for _, drainEvery := range []int{0, 1, 2, 5} {
					agg := NewAggregator(0, maxDepth)
					got := map[string]PrefixRow{}
					drain := func() {
						checkAggregatorAccounting(t, agg)
						rows := agg.Drain()
						for i := 1; i < len(rows); i++ {
							if rows[i-1].Prefix >= rows[i].Prefix {
								t.Fatalf("%s: drained %q then %q", name, rows[i-1].Prefix, rows[i].Prefix)
							}
						}
						mergeRows(got, rows)
					}
					for i, k := range seq {
						if segmentEvery > 0 && i%segmentEvery == 0 {
							agg.StartSegment()
						}
						agg.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
						if drainEvery > 0 && i%drainEvery == drainEvery-1 {
							drain()
						}
					}
					drain()
					if want := referenceAggregate(seq, maxDepth); !maps.Equal(want, got) {
						t.Fatalf("%s maxDepth=%d segmentEvery=%d drainEvery=%d (-reference +aggregator):\n%s",
							name, maxDepth, segmentEvery, drainEvery, gocmp.Diff(want, got))
					}
				}
			}
		}
	}
}

// checkAggregatorAccounting recomputes from the nodes what the
// aggregator has counted: the estimate (every node, its key and its tier
// block, the node table's slots, the spill's scratch). It checks that
// every tier block belongs to one node and every live node's totals are
// its tiers' sums, that the node table holds each live non-root node of
// the main tree once, under its key's hash, and that each node's stored
// key names its parent.
func checkAggregatorAccounting(t *testing.T, agg *Aggregator) {
	t.Helper()
	keyBytes := 0
	blockOwner := map[uint32]uint32{}
	for i := range agg.used {
		n := agg.node(uint32(i))
		if n.segLen > 0 {
			keyBytes += nodeKeyHeader + int(n.segLen)
		}
		if n.tiers != 0 {
			if owner, ok := blockOwner[n.tiers]; ok || int(n.tiers) > agg.blocksUsed {
				t.Fatalf("node %d has tier block %d of %d, node %d's too: %v", i, n.tiers, agg.blocksUsed, owner, ok)
			}
			blockOwner[n.tiers] = uint32(i)
		}
		if agg.parent(uint32(i)) == deadNode {
			continue
		}
		if b := agg.nodeBlock(n); b != nil {
			var count, size uint64
			for tier := range MaxTiers {
				count += b.counts[tier]
				size += b.bytes[tier]
			}
			if count != n.count || size != n.bytes {
				t.Fatalf("node %d has %d objects, %d bytes; its tiers add up to %d, %d", i, n.count, n.bytes, count, size)
			}
		}
	}
	if len(blockOwner) != agg.blocksUsed {
		t.Fatalf("%d of %d tier blocks belong to a node", len(blockOwner), agg.blocksUsed)
	}
	want := int64(agg.used)*aggregatorNodeBytes + int64(keyBytes) + int64(agg.blocksUsed)*tierBlockBytes
	if agg.bytes != want {
		t.Fatalf("counted %d node bytes; the nodes have %d", agg.bytes, want)
	}
	if got, want := agg.EstimatedMemoryUsage(), want+int64(len(agg.children.slots))*nodeSlotBytes+agg.scratchBytes(); got != want {
		t.Fatalf("estimate = %d, want %d with the node table and scratch", got, want)
	}
	if scratch := agg.scratchBytes(); scratch < walkScratchBytes*int64(agg.used) {
		t.Fatalf("scratch counts %d bytes for %d nodes", scratch, agg.used)
	}

	inTree := map[uint32]bool{}
	for _, r := range agg.mappedRanges(nil) {
		for i := r[0]; i < r[1]; i++ {
			if p := agg.parent(uint32(i)); p < deadNode {
				inTree[uint32(i)] = true
				n := agg.node(uint32(i))
				if got := binary.LittleEndian.Uint32(agg.segs.key(n.seg, n.segLen)); n.segLen > 0 && got != p {
					t.Fatalf("node %d's key names parent %d, its parent is %d", i, got, p)
				}
			}
		}
	}
	seen := map[uint32]bool{}
	for _, s := range agg.children.slots {
		if s.node == 0 {
			continue
		}
		idx := s.node - 1
		if !inTree[idx] || seen[idx] {
			t.Fatalf("node table holds node %d (in the main tree: %v, twice: %v)", idx, inTree[idx], seen[idx])
		}
		seen[idx] = true
		if want := agg.children.hash(agg.parent(idx), agg.segment(agg.node(idx))); s.hash != want {
			t.Fatalf("node %d is in the table under hash %#x, its key's is %#x", idx, s.hash, want)
		}
	}
	if len(seen) != len(inTree) || agg.children.count != len(seen) {
		t.Fatalf("node table holds %d nodes and counts %d; the main tree has %d", len(seen), agg.children.count, len(inTree))
	}
	if agg.children.count > maxTableLoad(len(agg.children.slots)) {
		t.Fatalf("node table holds %d nodes in %d slots, over the maximum load", agg.children.count, len(agg.children.slots))
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
// increasing, every row's totals are its tiers' sums, and the drains add
// up to a direct count.
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
							checkSpilledRows(t, fmt.Sprintf("seed=%d %s", seed, name), rows)
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
