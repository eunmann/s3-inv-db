package indexread_test

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestProfile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(3, 0)
	agg.AddObject("a/x.txt", 10, tiers.Standard)
	agg.AddObject("a/b/y.txt", 300, tiers.GlacierIR)
	agg.AddObject("c/z.txt", 5, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatal(err)
	}
	idx, err := indexread.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Prefixes in preorder: "" (3 objects), "a/" (2), "a/b/" (1), "c/" (1).
	p := idx.Profile()
	if p.Prefixes != 4 || p.Leaves != 2 {
		t.Errorf("prefixes, leaves = %d, %d; want 4, 2", p.Prefixes, p.Leaves)
	}
	if want := []uint64{1, 2, 1}; !slices.Equal(p.Depths, want) {
		t.Errorf("Depths = %v, want %v", p.Depths, want)
	}
	// "" and "a/" hold STANDARD and GLACIER_IR; "a/b/" and "c/" one each.
	if p.TiersPerPrefix[1] != 2 || p.TiersPerPrefix[2] != 2 {
		t.Errorf("TiersPerPrefix = %v, want 2 with one tier and 2 with two", p.TiersPerPrefix)
	}
	if p.TierSets != 3 {
		t.Errorf("TierSets = %d, want 3", p.TierSets)
	}
	checkBits := func(field string, want map[int]uint64) {
		t.Helper()
		f := p.Fields[field]
		var total uint64
		for b, n := range want {
			total += n
			if f.Bits[b] != n {
				t.Errorf("%s: Bits[%d] = %d, want %d", field, b, f.Bits[b], n)
			}
		}
		if f.Values != total {
			t.Errorf("%s: Values = %d, want %d", field, f.Values, total)
		}
	}
	checkBits(indexread.FieldObjectCount, map[int]uint64{2: 2, 1: 2})    // 3, 2, 1, 1
	checkBits(indexread.FieldParentDistance, map[int]uint64{1: 2, 2: 1}) // 1, 1, 3
	checkBits(indexread.FieldChildren, map[int]uint64{2: 1, 1: 1})       // "" has 2, "a/" has 1
	checkBits(indexread.FieldSubtreeSize, map[int]uint64{2: 1, 1: 1, 0: 2})
	// Tier bytes: "" 15 + 300, "a/" 10 + 300, "a/b/" 300, "c/" 5.
	checkBits(indexread.FieldTierBytes, map[int]uint64{4: 2, 9: 3, 3: 1})
}
