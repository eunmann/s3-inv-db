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

	// Prefixes in level order: "" (3 objects), "a/" (2), "c/" (1), "a/b/" (1).
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
	checkBits(indexread.FieldObjectCount, map[int]uint64{2: 2, 1: 2})      // 3, 2, 1, 1
	checkBits(indexread.FieldParentDistance, map[int]uint64{1: 1, 2: 2})   // 1, 2, 2
	checkBits(indexread.FieldChildren, map[int]uint64{2: 1, 1: 1})         // "" has 2, "a/" has 1
	checkBits(indexread.FieldChildStart, map[int]uint64{1: 1, 2: 1, 3: 2}) // 1, 3, 4, 4
	// Tier bytes: "" 15 + 300, "a/" 10 + 300, "a/b/" 300, "c/" 5.
	checkBits(indexread.FieldTierBytes, map[int]uint64{4: 2, 9: 3, 3: 1})
}

const quantileP999 = 0.999

func TestFieldProfileQuantileBits(t *testing.T) {
	tests := []struct {
		name    string
		bits    map[int]uint64
		q       float64
		want    int
		wantMax int
	}{
		{name: "empty", q: quantileP999, want: 0, wantMax: 0},
		{name: "median", bits: map[int]uint64{1: 2, 5: 2}, q: 0.5, want: 1, wantMax: 5},
		{name: "p99 of 500 below the outlier", bits: map[int]uint64{3: 499, 10: 1}, q: 0.99, want: 3, wantMax: 10},
		// 0.999 of 500 values is 499.5, so all 500 have to fit.
		{name: "p99.9 of 500 holds the outlier", bits: map[int]uint64{3: 499, 10: 1}, q: quantileP999, want: 10, wantMax: 10},
		{name: "p99.9 of 1000 excludes the outlier", bits: map[int]uint64{3: 999, 10: 1}, q: quantileP999, want: 3, wantMax: 10},
		{name: "zero values", bits: map[int]uint64{0: 3, 64: 1}, q: 0.75, want: 0, wantMax: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f indexread.FieldProfile
			for b, n := range tt.bits {
				f.Bits[b] = n
				f.Values += n
			}
			if got := f.QuantileBits(tt.q); got != tt.want {
				t.Errorf("QuantileBits(%v) = %d, want %d", tt.q, got, tt.want)
			}
			if got := f.MaxBits(); got != tt.wantMax {
				t.Errorf("MaxBits() = %d, want %d", got, tt.wantMax)
			}
		})
	}
}
