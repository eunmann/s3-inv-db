package indexread_test

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

func readHeaderCount(path string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	hb := make([]byte, format.HeaderSize)
	if _, err := f.ReadAt(hb, 0); err != nil {
		return 0
	}
	h, err := format.DecodeHeader(hb)
	if err != nil {
		return 0
	}
	return h.Count
}


// BenchmarkGrid sweeps the (shape × tier_dist × size_dist) grid and
// emits per-cell custom metrics so benchstat can diff disk and query
// effects of a format change across all cells in one pass.
//
// Per cell custom metrics:
//
//	bytes_per_prefix         — sum of every file under the index dir
//	tier_stats_Bpp           — per-prefix bytes for tier_stats_row.bin
//	core_stats_Bpp           — per-prefix bytes for core_stats.bin
//	mph_fp_pos_Bpp           — per-prefix bytes for mph_fp_pos.u64
//	prefix_dict_ids_Bpp      — per-prefix bytes for prefix_dict.ids.u32
//	prefix_off_Bpp           — per-prefix bytes for prefix_dict.prefix_off.u64
//	depth_positions_Bpp      — per-prefix bytes for depth_positions.u64
//	n_prefixes               — total prefix count after aggregation
//
// Inner sub-benchmarks time each query path on the same built index:
//
//	disk   — single-iteration timer; only the metrics matter
//	lookup — warm prefix-string lookup
//	stats  — warm StatsForPrefix
//	prefix — warm pos→string
//	tierbd — warm per-prefix tier breakdown
//	browse — warm Browse over a real depth-1 child set
//
// Toggles:
//
//	S3INV_GRID_N         — objects per cell (default 100_000)
//	S3INV_PREFIX_DICT=1  — build with prefix-dictionary (default off)
//	S3INV_FST=1          — build with FST prefix backend
//	S3INV_GRID_SHAPES    — comma-separated subset
//	S3INV_GRID_TIERS     — comma-separated subset
//	S3INV_GRID_SIZES     — comma-separated subset
// BenchmarkGridDisk emits only the per-file disk metrics. One build
// per cell, no query setup — fast enough to iterate on format changes
// (typically ~30s for the full 100-cell grid at n=100K).
//
//	go test -bench=BenchmarkGridDisk -benchtime=1x ./pkg/indexread/
func BenchmarkGridDisk(b *testing.B) {
	silenceZerolog(b)
	n := envInt("S3INV_GRID_N", 100_000)
	for _, shape := range filterStr(benchutil.ShapesForGrid(), os.Getenv("S3INV_GRID_SHAPES")) {
		for _, td := range filterTier(benchutil.TierDistributions(), os.Getenv("S3INV_GRID_TIERS")) {
			for _, sd := range filterSize(benchutil.SizeDistributions(), os.Getenv("S3INV_GRID_SIZES")) {
				name := fmt.Sprintf("shape=%s/tier=%s/size=%s", shape, td.Name, sd.Name)
				b.Run(name, func(b *testing.B) {
					spec := benchutil.GridSpec{
						Shape: shape, Tier: td, Size: sd, N: n, Seed: benchutil.BenchmarkSeed,
					}
					dir := buildGridIndex(b, spec)
					defer os.RemoveAll(dir)
					files := walkDirSizes(b, dir)
					nPx := readPrefixCount(b, dir, files)
					for range b.N {
						reportFileMetrics(b, files, nPx)
					}
				})
			}
		}
	}
}

// BenchmarkGridQuery emits per-cell query latencies (warm Lookup,
// StatsForPrefix, PrefixString, TierBreakdown, Browse). Setup picks
// only a small sample of queries to keep per-cell overhead bounded.
//
//	go test -bench=BenchmarkGridQuery -benchtime=20ms ./pkg/indexread/
func BenchmarkGridQuery(b *testing.B) {
	silenceZerolog(b)
	n := envInt("S3INV_GRID_N", 100_000)
	for _, shape := range filterStr(benchutil.ShapesForGrid(), os.Getenv("S3INV_GRID_SHAPES")) {
		for _, td := range filterTier(benchutil.TierDistributions(), os.Getenv("S3INV_GRID_TIERS")) {
			for _, sd := range filterSize(benchutil.SizeDistributions(), os.Getenv("S3INV_GRID_SIZES")) {
				name := fmt.Sprintf("shape=%s/tier=%s/size=%s", shape, td.Name, sd.Name)
				b.Run(name, func(b *testing.B) {
					spec := benchutil.GridSpec{
						Shape: shape, Tier: td, Size: sd, N: n, Seed: benchutil.BenchmarkSeed,
					}
					runGridQueryCell(b, spec)
				})
			}
		}
	}
}

func runGridQueryCell(b *testing.B, spec benchutil.GridSpec) {
	b.Helper()
	dir := buildGridIndex(b, spec)
	defer os.RemoveAll(dir)
	files := walkDirSizes(b, dir)
	nPx := readPrefixCount(b, dir, files)

	idx, err := indexread.Open(dir)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer idx.Close()

	queries, positions := pickGridQueries(b, idx, nPx, 200)
	kids := pickLargestDepthOneSubtreeCapped(idx, queries, 20)

	b.Run("lookup", func(b *testing.B) {
		reportFileMetrics(b, files, nPx)
		b.ResetTimer()
		for i := range b.N {
			_, _ = idx.Lookup(queries[i%len(queries)])
		}
	})
	b.Run("stats", func(b *testing.B) {
		reportFileMetrics(b, files, nPx)
		b.ResetTimer()
		for i := range b.N {
			_, _ = idx.StatsForPrefix(queries[i%len(queries)])
		}
	})
	b.Run("prefix", func(b *testing.B) {
		reportFileMetrics(b, files, nPx)
		if len(positions) == 0 {
			b.Skip("no positions")
		}
		b.ResetTimer()
		for i := range b.N {
			_, _ = idx.PrefixString(positions[i%len(positions)])
		}
	})
	b.Run("tierbd", func(b *testing.B) {
		reportFileMetrics(b, files, nPx)
		if len(positions) == 0 {
			b.Skip("no positions")
		}
		b.ResetTimer()
		for i := range b.N {
			_ = idx.TierBreakdown(positions[i%len(positions)])
		}
	})
	b.Run(fmt.Sprintf("browse/kids=%d", len(kids)), func(b *testing.B) {
		reportFileMetrics(b, files, nPx)
		if len(kids) == 0 {
			b.Skip("no children")
		}
		b.ResetTimer()
		for range b.N {
			for _, pos := range kids {
				_, _ = idx.PrefixString(pos)
				_ = idx.Stats(pos)
				_ = idx.TierBreakdown(pos)
			}
		}
	})
}

func pickLargestDepthOneSubtreeCapped(idx *indexread.Index, candidates []string, maxScan int) []uint64 {
	var best []uint64
	scanned := 0
	for _, p := range candidates {
		if scanned >= maxScan {
			break
		}
		scanned++
		pos, ok := idx.Lookup(p)
		if !ok {
			continue
		}
		kids, err := idx.DescendantsAtDepth(pos, 1)
		if err == nil && len(kids) > len(best) {
			best = kids
		}
	}
	if root, err := idx.DescendantsAtDepth(0, 1); err == nil && len(root) > len(best) {
		best = root
	}
	return best
}

// buildGridIndex constructs an index for spec, honouring the same env
// toggles the legacy bench helpers use.
func buildGridIndex(tb testing.TB, spec benchutil.GridSpec) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "grid-*")
	if err != nil {
		tb.Fatalf("mktmp: %v", err)
	}

	agg := extsort.NewAggregator(spec.N, 0)
	gen := benchutil.NewGridGenerator(spec)
	gen.Stream(spec.N, func(o benchutil.FakeObject) {
		agg.AddObject(o.Key, o.Size, o.TierID)
	})
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)))
	if err != nil {
		tb.Fatalf("NewIndexBuilder: %v", err)
	}
	if os.Getenv("S3INV_PREFIX_DICT") == "1" {
		if err := builder.SetPrefixDictionary(true); err != nil {
			tb.Fatalf("SetPrefixDictionary: %v", err)
		}
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		tb.Fatalf("SetPresentTiers: %v", err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			tb.Fatalf("Add: %v", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		tb.Fatalf("Finalize: %v", err)
	}

	return dir
}

func walkDirSizes(tb testing.TB, dir string) map[string]int64 {
	tb.Helper()
	out := map[string]int64{}
	if err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		i, _ := d.Info()
		rel, _ := filepath.Rel(dir, p)
		out[rel] = i.Size()
		return nil
	}); err != nil {
		tb.Fatalf("walk: %v", err)
	}
	return out
}

func readPrefixCount(tb testing.TB, dir string, files map[string]int64) uint64 {
	tb.Helper()
	// core_stats.bin is the most reliable count source; fall back to
	// any other header-bearing file.
	for _, name := range []string{"core_stats.bin", "depth_positions.u64", "mph_fp_pos.u64"} {
		if _, ok := files[name]; !ok {
			continue
		}
		if n := readHeaderCount(filepath.Join(dir, name)); n > 0 {
			return n
		}
	}
	return 0
}

func reportFileMetrics(b *testing.B, files map[string]int64, nPx uint64) {
	b.Helper()
	if nPx == 0 {
		return
	}
	var total int64
	for _, sz := range files {
		total += sz
	}
	b.ReportMetric(float64(nPx), "n_prefixes")
	b.ReportMetric(float64(total)/float64(nPx), "Bpp_total")
	for _, e := range gridReportedFiles {
		sz, ok := files[e.name]
		if ok {
			b.ReportMetric(float64(sz)/float64(nPx), e.metric)
		} else if e.required {
			// Surface absent files as 0 so benchstat doesn't drop the column.
			b.ReportMetric(0, e.metric)
		}
	}
}

type gridFile struct {
	name     string
	metric   string
	required bool
}

var gridReportedFiles = []gridFile{
	{"tier_stats/tier_stats_row.bin", "Bpp_tier", true},
	{"core_stats.bin", "Bpp_core", true},
	{"mph_fp_pos.u64", "Bpp_fpp", false},
	{"prefix_dict.ids.u32", "Bpp_ids", false},
	{"prefix_dict.prefix_off.u64", "Bpp_poff", false},
	{"depth_positions.u64", "Bpp_dpos", false},
	{"prefix_blob.bin", "Bpp_pblob", false},
}

func pickGridQueries(b *testing.B, idx *indexread.Index, nPx uint64, qmax int) ([]string, []uint64) {
	b.Helper()
	if nPx == 0 {
		return nil, nil
	}
	limit := int(nPx)
	if limit > qmax {
		limit = qmax
	}
	rng := rand.New(rand.NewSource(7))
	queries := make([]string, 0, limit)
	positions := make([]uint64, 0, limit)
	seen := map[uint64]struct{}{}
	for len(queries) < limit {
		p := uint64(rng.Intn(int(nPx)))
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		s, err := idx.PrefixString(p)
		if err != nil || s == "" {
			continue
		}
		queries = append(queries, s)
		positions = append(positions, p)
	}
	return queries, positions
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

func filterStr(all []string, csv string) []string {
	if csv == "" {
		return all
	}
	want := splitCSV(csv)
	out := make([]string, 0, len(want))
	for _, w := range want {
		for _, a := range all {
			if a == w {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

func filterTier(all []benchutil.TierDistribution, csv string) []benchutil.TierDistribution {
	if csv == "" {
		return all
	}
	want := splitCSV(csv)
	out := make([]benchutil.TierDistribution, 0, len(want))
	for _, w := range want {
		for _, a := range all {
			if a.Name == w {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

func filterSize(all []benchutil.SizeDistribution, csv string) []benchutil.SizeDistribution {
	if csv == "" {
		return all
	}
	want := splitCSV(csv)
	out := make([]benchutil.SizeDistribution, 0, len(want))
	for _, w := range want {
		for _, a := range all {
			if a.Name == w {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	cur := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if v := s[cur:i]; v != "" {
				out = append(out, v)
			}
			cur = i + 1
		}
	}
	return out
}
