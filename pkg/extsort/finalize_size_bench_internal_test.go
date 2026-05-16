package extsort

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// BenchmarkIndexBuilder_FinalizeSize is more "report" than "bench" —
// it builds an index of n synthetic prefixes and reports the
// resulting on-disk byte count + per-prefix overhead. Used as a
// before/after harness for the on-disk density work in Wave 1
// (depth-uint8, max-depth-uint8, subtree-end uint32) and Wave 2
// (sparse tier stats).
func BenchmarkIndexBuilder_FinalizeSize(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			runFinalizeSizeBench(b, n)
		})
	}
}

func runFinalizeSizeBench(b *testing.B, n int) {
	b.Helper()
	b.ReportAllocs()
	keys := generateBenchKeys(n, 4)
	for range b.N {
		dir := b.TempDir()
		agg := NewAggregator(0, 0)
		for _, k := range keys {
			agg.AddObject(k, 1024, tiers.Standard)
		}
		rows := agg.Drain()
		SortPrefixRows(rows)

		builder, err := NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)), false)
		if err != nil {
			b.Fatalf("NewIndexBuilderWithCapacity: %v", err)
		}
		for _, row := range rows {
			if err := builder.Add(row); err != nil {
				b.Fatalf("Add: %v", err)
			}
		}
		if err := builder.Finalize(); err != nil {
			b.Fatalf("Finalize: %v", err)
		}

		size := dirBytes(b, dir)
		b.ReportMetric(float64(size), "index_bytes")
		b.ReportMetric(float64(size)/float64(len(rows)), "bytes/prefix")
	}
}

// BenchmarkTierStats_Density measures index size when prefixes use
// only a small fraction of the available tiers. Today the tier
// arrays are dense (one slot per (tier, prefix) pair regardless of
// whether the prefix has any objects in that tier), which wastes
// space when a typical bucket is dominated by STANDARD.
func BenchmarkTierStats_Density(b *testing.B) {
	cases := []struct {
		name      string
		tiersUsed int
	}{
		{"sparse_1tier", 1}, // STANDARD-only
		{"mixed_3tiers", 3},
		{"all_tiers", int(tiers.NumTiers)},
	}
	const n = 100_000
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			keys := generateBenchKeys(n, 4)
			for range b.N {
				dir := b.TempDir()
				agg := NewAggregator(0, 0)
				for i, k := range keys {
					tierID := tiers.ID(i % c.tiersUsed)
					agg.AddObject(k, 1024, tierID)
				}
				rows := agg.Drain()
				SortPrefixRows(rows)
				builder, err := NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)), false)
				if err != nil {
					b.Fatalf("NewIndexBuilderWithCapacity: %v", err)
				}
				for _, row := range rows {
					if err := builder.Add(row); err != nil {
						b.Fatalf("Add: %v", err)
					}
				}
				if err := builder.Finalize(); err != nil {
					b.Fatalf("Finalize: %v", err)
				}
				size := dirBytes(b, dir)
				b.ReportMetric(float64(size), "index_bytes")
				b.ReportMetric(float64(size)/float64(len(rows)), "bytes/prefix")
			}
		})
	}
}

// dirBytes sums every regular file under dir. Used by the size
// benches above; tier_stats files live in a subdir so a recursive
// walk is needed.
func dirBytes(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})
	if err != nil {
		b.Fatalf("walk %s: %v", dir, err)
	}

	return total
}

// suppress unused import false-positive on strconv when generateBenchKeys
// isn't on this file's path. Real call lives in aggregator_bench_internal_test.go.
var _ = strconv.Itoa
