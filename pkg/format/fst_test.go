package format_test

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestFSTIndex_RoundTrips builds an FST-backed index from a realistic
// fixture and verifies every prefix round-trips through Lookup and
// GetPrefix. Catches off-by-one errors in the anchor walk before any
// benchmarking runs.
func TestFSTIndex_RoundTrips(t *testing.T) {
	for _, n := range []int{1, 64, 65, 100, 10_000} {
		t.Run("n="+itoa(n), func(t *testing.T) {
			dir := buildFSTFixture(t, n)
			m, err := format.OpenMPHF(dir)
			if err != nil {
				t.Fatalf("OpenMPHF: %v", err)
			}
			defer m.Close()
			if got := m.Count(); got == 0 {
				t.Fatalf("empty count")
			}
			if err := format.VerifyMPHF(m); err != nil {
				t.Fatalf("VerifyMPHF: %v", err)
			}
		})
	}
}

// TestFSTIndex_MissingPrefixReturnsFalse confirms negative lookups
// don't bleed into the position table.
func TestFSTIndex_MissingPrefixReturnsFalse(t *testing.T) {
	dir := buildFSTFixture(t, 10_000)
	m, err := format.OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()
	for _, miss := range []string{"_does_not_exist/", "zzz/zzz/zzz/zzz/", "\x00bogus"} {
		if pos, ok := m.Lookup(miss); ok {
			t.Errorf("Lookup(%q) = %d, true; want ok=false", miss, pos)
		}
	}
}

func buildFSTFixture(tb testing.TB, n int) string {
	tb.Helper()
	dir := filepath.Join(tb.TempDir(), "idx")

	agg := extsort.NewAggregator(n, 0)
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	for _, o := range gen.Generate() {
		agg.AddObject(o.Key, o.Size, o.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	b, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		tb.Fatalf("NewIndexBuilder: %v", err)
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

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
