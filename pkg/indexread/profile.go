package indexread

import (
	"math"
	"math/bits"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Profile describes the data an index holds, independent of how the
// index lays it out: how many tiers each prefix has, how wide each
// per-prefix value is, the tree's shape and the segment dictionary.
// Layout decisions are made from these numbers, so they are taken from
// real indexes rather than assumed.
type Profile struct {
	// Depths[d] is the number of prefixes at depth d.
	Depths []uint64 `json:"depths"`
	// TiersPerPrefix[k] is the number of prefixes with exactly k tiers
	// holding data.
	TiersPerPrefix []uint64 `json:"tiers_per_prefix"`
	// TierSets is the number of distinct combinations of tiers that
	// occur on a prefix.
	TierSets int `json:"tier_sets"`
	// Fields holds, per per-prefix value, how many bits each row needs.
	Fields map[string]*FieldProfile `json:"fields"`
	// Prefixes is the number of prefixes, the root included.
	Prefixes uint64 `json:"prefixes"`
	// Leaves is the number of prefixes with no child prefix.
	Leaves uint64 `json:"leaves"`
	// Segments is the number of distinct segments in the dictionary,
	// and SegmentBytes their total length.
	Segments     uint64 `json:"segments"`
	SegmentBytes uint64 `json:"segment_bytes"`
}

// Profile field names.
const (
	FieldObjectCount    = "object_count"
	FieldTotalBytes     = "total_bytes"
	FieldChildStart     = "child_start"
	FieldParentDistance = "parent_distance"
	FieldChildren       = "children"
	FieldTierCount      = "tier_count"
	FieldTierBytes      = "tier_bytes"
)

// FieldProfile is the distribution of one value's bit length over the
// rows that carry it.
type FieldProfile struct {
	// Bits[b] is the number of values that need exactly b bits (0 for
	// the value 0).
	Bits [65]uint64 `json:"bits"`
	// Values is the number of values recorded.
	Values uint64 `json:"values"`
}

func (f *FieldProfile) add(v uint64) {
	f.Bits[bits.Len64(v)]++
	f.Values++
}

// MaxBits is the bit length of the largest value.
func (f *FieldProfile) MaxBits() int {
	for b := len(f.Bits) - 1; b > 0; b-- {
		if f.Bits[b] > 0 {
			return b
		}
	}

	return 0
}

// QuantileBits is the smallest bit length that holds at least the
// fraction q of the values.
func (f *FieldProfile) QuantileBits(q float64) int {
	need := uint64(math.Ceil(q * float64(f.Values)))
	var seen uint64
	for b, n := range &f.Bits {
		seen += n
		if seen >= need {
			return b
		}
	}

	return len(f.Bits) - 1
}

// Profile scans every prefix once. It reads the whole index, so its
// cost is proportional to the index size.
func (idx *Index) Profile() *Profile {
	p := &Profile{
		Prefixes:       idx.count,
		Depths:         make([]uint64, idx.maxDepth+1),
		TiersPerPrefix: make([]uint64, tiers.NumTiers+1),
		Fields:         map[string]*FieldProfile{},
	}
	for _, name := range []string{
		FieldObjectCount, FieldTotalBytes, FieldChildStart,
		FieldParentDistance, FieldChildren, FieldTierCount, FieldTierBytes,
	} {
		p.Fields[name] = &FieldProfile{}
	}
	seg := idx.prefixes.SegmentStats()
	p.Segments, p.SegmentBytes = seg.Count, seg.Bytes

	tierSets := map[uint16]struct{}{}
	for pos := range idx.count {
		st := idx.Stats(pos)
		p.Fields[FieldObjectCount].add(st.ObjectCount)
		p.Fields[FieldTotalBytes].add(st.TotalBytes)
		p.Depths[idx.Depth(pos)]++
		// Rows are in level order: pos's children are the rows from its
		// child start to the next row's.
		first, end := idx.coreStats.UnsafeChildStart(pos), idx.coreStats.UnsafeChildStart(pos+1)
		p.Fields[FieldChildStart].add(first)
		if end == first {
			p.Leaves++
		} else {
			p.Fields[FieldChildren].add(end - first)
		}
		for c := first; c < end; c++ {
			p.Fields[FieldParentDistance].add(c - pos)
		}

		var set uint16
		for _, t := range idx.TierBreakdown(pos) {
			if t.ObjectCount == 0 && t.Bytes == 0 {
				continue
			}
			set |= 1 << t.TierID
			p.Fields[FieldTierCount].add(t.ObjectCount)
			p.Fields[FieldTierBytes].add(t.Bytes)
		}
		p.TiersPerPrefix[bits.OnesCount16(set)]++
		tierSets[set] = struct{}{}
	}
	p.TierSets = len(tierSets)

	return p
}
