package extsort

import (
	"bytes"
	"fmt"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	gocmp "github.com/google/go-cmp/cmp"
)

// tierLayout is a sequence of objects for the aggregator: key i in tier
// tierOf(i), a StartSegment before each key in segmentAt and a spill
// after each key in spillAfter.
type tierLayout struct {
	keys       []string
	tierOf     func(i int) tiers.ID
	segmentAt  map[int]bool
	spillAfter map[int]bool
}

// everyN returns the indexes below n that are off modulo every.
func everyN(n, every, off int) map[int]bool {
	out := map[int]bool{}
	for i := off; i < n; i += every {
		out[i] = true
	}

	return out
}

// tierLayouts are the ways an object's tier and the aggregator's runs
// can meet: every object in one tier, the lowest or the highest; tiers
// cycling through all of them; an ordered segment in one tier mapped
// into, or merged at a spill with, main-tree nodes of another; and
// spills right at, right after and inside segments.
func tierLayouts() map[string]tierLayout {
	rng := rand.New(rand.NewPCG(14, 15))
	keys := pathTestKeys(rng, 3000)
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	chunkSorted := slices.Clone(keys)
	for i := 0; i < len(chunkSorted); i += 300 {
		slices.Sort(chunkSorted[i:min(i+300, len(chunkSorted))])
	}
	n := len(keys)
	const last = tiers.NumTiers - 1
	one := func(t tiers.ID) func(int) tiers.ID { return func(int) tiers.ID { return t } }
	cycle := func(i int) tiers.ID { return tiers.ID(i % int(tiers.NumTiers)) }

	// The first third, out of order, builds the main tree in one tier;
	// the rest comes in segments of sorted keys under the same prefixes
	// in another.
	split := n / 3
	intoTree := slices.Clone(keys[:split])
	rest := slices.Clone(keys[split:])
	slices.Sort(rest)
	intoTree = append(intoTree, rest...)
	segmented := everyN(n, 500, split)
	// One key out of order in each segment maps it into the main tree.
	mapped := slices.Clone(intoTree)
	for s := split; s+260 < n; s += 500 {
		mapped[s+250], mapped[s+10] = mapped[s+10], mapped[s+250]
	}
	twoTiers := func(i int) tiers.ID {
		if i < split {
			return tiers.Standard
		}

		return tiers.GlacierFR
	}
	// Segment objects alternate two tiers, so a node of the segment
	// with both maps into a main-tree node with one, and the other way.
	mixedSegments := func(i int) tiers.ID {
		switch {
		case i < split:
			return tiers.ITArchive
		case i%2 == 0:
			return tiers.DeepArchive
		}

		return last
	}
	atBoundaries := map[int]bool{}
	for i := range segmented {
		if i > 0 {
			atBoundaries[i-1] = true // the segment's keys are all spilled
		}
		atBoundaries[i] = true // after its first key
		atBoundaries[i+7] = true
	}

	return map[string]tierLayout{
		"one_tier_lowest/random":          {keys: keys, tierOf: one(tiers.Standard), spillAfter: everyN(n, 701, 700)},
		"one_tier_highest/random":         {keys: keys, tierOf: one(last), spillAfter: everyN(n, 701, 700)},
		"one_tier_highest/segments":       {keys: chunkSorted, tierOf: one(last), segmentAt: everyN(n, 300, 0)},
		"cycling/random":                  {keys: keys, tierOf: cycle, spillAfter: everyN(n, 997, 996)},
		"cycling/sorted":                  {keys: sorted, tierOf: cycle},
		"cycling/segments":                {keys: chunkSorted, tierOf: cycle, segmentAt: everyN(n, 300, 0), spillAfter: everyN(n, 1013, 1012)},
		"segment_into_tree/mapped":        {keys: mapped, tierOf: twoTiers, segmentAt: segmented},
		"segment_into_tree/merged":        {keys: intoTree, tierOf: twoTiers, segmentAt: segmented},
		"segment_into_tree/mixed_mapped":  {keys: mapped, tierOf: mixedSegments, segmentAt: segmented},
		"segment_into_tree/mixed_merged":  {keys: intoTree, tierOf: mixedSegments, segmentAt: segmented},
		"spills_between_segments/mapped":  {keys: mapped, tierOf: mixedSegments, segmentAt: segmented, spillAfter: atBoundaries},
		"spills_between_segments/merged":  {keys: intoTree, tierOf: cycle, segmentAt: segmented, spillAfter: atBoundaries},
		"spills_between_segments/chunked": {keys: chunkSorted, tierOf: cycle, segmentAt: everyN(n, 300, 0), spillAfter: everyN(n, 300, 299)},
	}
}

// TestAggregatorTierLayoutsAreExact: for every tier layout, spilled
// through Drain, through WriteRun or as a memory run, every spill is
// strictly increasing, every row's totals are its tiers' sums, and the
// spills add up to a direct count. Every run file WriteRun writes is
// byte for byte the one Drain's rows written one by one make.
func TestAggregatorTierLayoutsAreExact(t *testing.T) {
	dir := t.TempDir()
	seen := map[string]int{}
	for name, l := range tierLayouts() {
		for _, mode := range []string{"drain", "run file", "memory run"} {
			where := fmt.Sprintf("%s via %s", name, mode)
			agg, ref := NewAggregator(0, 0), NewAggregator(0, 0)
			got := map[string]PrefixRow{}
			spill := func() {
				for _, seg := range agg.segments {
					if seg.mapped {
						seen["mapped segment"]++
					}
				}
				if _, one := agg.soleRun(); !one && agg.PrefixCount() > 0 {
					seen["several runs"]++
				}
				var rows []*PrefixRow
				switch mode {
				case "drain":
					rows = agg.Drain()
				case "memory run":
					rows = memRunRows(t, agg)
				default:
					path := filepath.Join(dir, "run.crun")
					written := writeTestRun(t, path, func(w *CompressedRunWriter) error {
						_, err := agg.WriteRun(w)

						return err
					})
					drained := ref.Drain()
					if want := writeTestRun(t, filepath.Join(dir, "drained.crun"), func(w *CompressedRunWriter) error {
						return w.WriteAll(drained)
					}); !bytes.Equal(written, want) {
						t.Fatalf("%s: WriteRun's run file (%d B) differs from Drain+WriteAll's (%d B)", where, len(written), len(want))
					}
					for _, r := range readTestRun(t, path) {
						rows = append(rows, &r)
					}
				}
				checkSpilledRows(t, where, rows)
				mergeRows(got, rows)
			}
			for i, k := range l.keys {
				if l.segmentAt[i] {
					agg.StartSegment()
					ref.StartSegment()
				}
				agg.AddObject(k, uint64(len(k)+i), l.tierOf(i))
				ref.AddObject(k, uint64(len(k)+i), l.tierOf(i))
				if l.spillAfter[i] {
					spill()
				}
			}
			spill()
			if want := referenceAggregateTiers(l.keys, 0, l.tierOf); !maps.Equal(want, got) {
				t.Fatalf("%s (-reference +aggregator):\n%s", where, gocmp.Diff(want, got))
			}
		}
	}
	if seen["mapped segment"] == 0 || seen["several runs"] == 0 {
		t.Errorf("the layouts never map a segment or never spill several runs: %v", seen)
	}
}

// checkSpilledRows fails unless rows are strictly increasing by prefix
// and each row's totals are the sums of its tiers'.
func checkSpilledRows(t *testing.T, where string, rows []*PrefixRow) {
	t.Helper()
	for i, r := range rows {
		if i > 0 && rows[i-1].Prefix >= r.Prefix {
			t.Fatalf("%s: spilled %q then %q", where, rows[i-1].Prefix, r.Prefix)
		}
		var count, size uint64
		for tier := range MaxTiers {
			count += r.TierCounts[tier]
			size += r.TierBytes[tier]
		}
		if count != r.Count || size != r.TotalBytes {
			t.Fatalf("%s: row %q has %d objects, %d bytes; its tiers add up to %d, %d", where, r.Prefix, r.Count, r.TotalBytes, count, size)
		}
	}
}
