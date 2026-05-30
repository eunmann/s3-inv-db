package format_test

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestFSTOptionA_RoundTrips builds an index with FST forward lookup
// enabled and verifies every prefix Lookups back to the right pos
// and every pos GetPrefixes back to the original string.
//
// Tests both prefix-blob and prefix-dict reverse stores so the
// FST-sidecar plumbing is exercised against both.
func TestFSTOptionA_RoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name string
		dict bool
		n    int
	}{
		{name: "blob_n1", dict: false, n: 1},
		{name: "blob_n100", dict: false, n: 100},
		{name: "dict_n1", dict: true, n: 1},
		{name: "dict_n100", dict: true, n: 100},
		{name: "dict_n10000", dict: true, n: 10_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := buildFSTIndex(t, tc.dict, tc.n)
			mphf, err := format.OpenMPHF(dir)
			if err != nil {
				t.Fatalf("OpenMPHF: %v", err)
			}
			defer mphf.Close()

			if mphf.Count() == 0 {
				t.Fatalf("empty index")
			}

			// VerifyMPHF round-trips every position: GetPrefix(i)
			// must Lookup back to i.
			if err := format.VerifyMPHF(mphf); err != nil {
				t.Fatalf("VerifyMPHF: %v", err)
			}
		})
	}
}

// TestFSTOptionA_MissingPrefixReturnsFalse confirms negative
// lookups for absent prefixes return ok=false instead of a bogus
// position. (Vellum's Get is exact, so this should be trivial — but
// keep it as a regression gate.)
func TestFSTOptionA_MissingPrefixReturnsFalse(t *testing.T) {
	dir := buildFSTIndex(t, true, 1000)
	mphf, err := format.OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer mphf.Close()
	for _, miss := range []string{"_does_not_exist/", "zzz/zzz/zzz/zzz/", "\x00bogus"} {
		if pos, ok := mphf.Lookup(miss); ok {
			t.Errorf("Lookup(%q) = %d, true; want ok=false", miss, pos)
		}
	}
}

// buildFSTIndex builds a small index with FST forward lookup
// enabled. Uses the s3_dated grid generator at the requested n.
func buildFSTIndex(tb testing.TB, useDict bool, n int) string {
	tb.Helper()
	dir := filepath.Join(tb.TempDir(), "idx")

	agg := extsort.NewAggregator(n, 0)
	spec := benchutil.GridSpec{
		Shape: "s3_dated",
		Tier:  benchutil.SingleTier(),
		Size:  benchutil.SmallUniform(),
		N:     n,
		Seed:  benchutil.BenchmarkSeed,
	}
	gen := benchutil.NewGridGenerator(spec)
	gen.Stream(n, func(o benchutil.FakeObject) {
		agg.AddObject(o.Key, o.Size, o.TierID)
	})
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	b, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		tb.Fatalf("NewIndexBuilder: %v", err)
	}
	if useDict {
		if err := b.SetPrefixDictionary(true); err != nil {
			tb.Fatalf("SetPrefixDictionary: %v", err)
		}
	}
	if err := b.SetUseFST(true); err != nil {
		tb.Fatalf("SetUseFST: %v", err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		tb.Fatalf("SetPresentTiers: %v", err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			tb.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finalize(); err != nil {
		tb.Fatalf("Finalize: %v", err)
	}

	return dir
}
