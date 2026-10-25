package indexread_test

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkCache* measure what each query type costs the memory system,
// not only the CPU, so index layout changes can be judged on page and
// cache traffic. Every query type runs:
//
//   - state=warm: the index stays open and its pages stay cached; ns/q
//     is then dominated by CPU-cache and TLB misses.
//   - state=cold: before every op the index is closed, its files are
//     evicted from the page cache and it is reopened (untimed); the op
//     then runs cacheColdQueries queries. majflt/q and ioKiB/q are what
//     the layout makes each query read from storage, and <file>KiB/q is
//     how much of each file the queries mapped in.
//
// Keys come from a seeded sample of positions in random order, either
// uniform or Zipf-skewed (a few hot prefixes, as a dashboard sees).
//
// A cold op reopens the index so its mappings start empty: smaps Rss
// then counts only the pages that op's queries mapped in. The fixture
// is cacheObjects objects (~2.4M prefixes); S3INV_LONG_BENCH adds one
// of ~10M prefixes, past the CPU caches and the TLB's reach.

const (
	cacheObjects     = 1_000_000
	cacheLongObjects = 4_200_000
	cacheKeys        = 100_000 // keys per warm op
	cacheColdQueries = 1_000   // queries per cold op
	cacheParents     = 1_000
	cachePageRows    = 50 // rows a browse page shows
	cacheZipfS       = 1.1
)

type cacheFixture struct {
	dir string
	// Per distribution: positions and their prefix strings in query
	// order, and positions that have children.
	keys    map[string][]uint64
	strs    map[string][]string
	parents map[string][]uint64
}

var (
	cacheFixMu sync.Mutex
	cacheFixes = map[int]*cacheFixture{}
)

func cacheFixtureFor(b *testing.B, objects int) *cacheFixture {
	b.Helper()
	cacheFixMu.Lock()
	defer cacheFixMu.Unlock()
	if f, ok := cacheFixes[objects]; ok {
		return f
	}
	dir, err := buildCacheFixtureIndex(objects)
	if err != nil {
		b.Fatalf("cache fixture: %v", err)
	}
	f, err := sampleCacheKeys(dir)
	if err != nil {
		b.Fatalf("cache fixture keys: %v", err)
	}
	cacheFixes[objects] = f

	return f
}

func buildCacheFixtureIndex(objects int) (string, error) {
	dir := filepath.Join(gateTempRoot(), fmt.Sprintf("cache-%d", objects))
	if err := buildIndexAt(dir, objects); err != nil {
		return "", err
	}

	return dir, nil
}

func sampleCacheKeys(dir string) (*cacheFixture, error) {
	idx, err := indexread.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer idx.Close()

	r := rand.New(rand.NewPCG(1, 2))
	n := idx.Count()
	sample := make([]uint64, cacheKeys)
	for i := range sample {
		sample[i] = r.Uint64N(n)
	}
	zipf := rand.NewZipf(r, cacheZipfS, 1, uint64(len(sample)-1))
	skewed := make([]uint64, cacheKeys)
	for i := range skewed {
		skewed[i] = sample[zipf.Uint64()]
	}

	f := &cacheFixture{
		dir:     dir,
		keys:    map[string][]uint64{"uniform": sample, "zipf": skewed},
		strs:    map[string][]string{},
		parents: map[string][]uint64{},
	}
	for dist, keys := range f.keys {
		strs := make([]string, len(keys))
		for i, pos := range keys {
			if strs[i], err = idx.PrefixString(pos); err != nil {
				return nil, fmt.Errorf("prefix string: %w", err)
			}
			if len(f.parents[dist]) < cacheParents && idx.SubtreeEnd(pos) > pos {
				f.parents[dist] = append(f.parents[dist], pos)
			}
		}
		f.strs[dist] = strs
	}

	return f, nil
}

// cacheQuery runs query i of one type against idx.
type cacheQuery func(b *testing.B, idx *indexread.Index, f *cacheFixture, dist string, i int)

func BenchmarkCacheStatsForPrefix(b *testing.B) {
	runCacheBench(b, func(b *testing.B, idx *indexread.Index, f *cacheFixture, dist string, i int) {
		b.Helper()
		strs := f.strs[dist]
		if _, ok := idx.StatsForPrefix(strs[i%len(strs)]); !ok {
			b.Fatal("prefix not found")
		}
	}, false)
}

func BenchmarkCacheTierBreakdown(b *testing.B) {
	runCacheBench(b, func(_ *testing.B, idx *indexread.Index, f *cacheFixture, dist string, i int) {
		keys := f.keys[dist]
		_ = idx.TierBreakdown(keys[i%len(keys)])
	}, false)
}

func BenchmarkCachePrefixString(b *testing.B) {
	runCacheBench(b, func(b *testing.B, idx *indexread.Index, f *cacheFixture, dist string, i int) {
		b.Helper()
		keys := f.keys[dist]
		if _, err := idx.PrefixString(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}, false)
}

// BenchmarkCacheBrowsePage: one browse page per query, the first
// cachePageRows children of a parent with each row's stats, tiers
// and name.
func BenchmarkCacheBrowsePage(b *testing.B) {
	runCacheBench(b, func(b *testing.B, idx *indexread.Index, f *cacheFixture, dist string, i int) {
		b.Helper()
		parents := f.parents[dist]
		page, err := idx.DescendantsPage(parents[i%len(parents)], 1, indexread.Filter{}, 0, cachePageRows)
		if err != nil {
			b.Fatal(err)
		}
		for _, pos := range page.Positions {
			_ = idx.Stats(pos)
			_ = idx.TierBreakdown(pos)
			_ = idx.ChildSegment(pos)
			if _, err := idx.PrefixString(pos); err != nil {
				b.Fatal(err)
			}
		}
	}, true)
}

func runCacheBench(b *testing.B, q cacheQuery, perParent bool) {
	b.Helper()
	sizes := []int{cacheObjects}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, cacheLongObjects)
	}
	for _, objects := range sizes {
		for _, dist := range []string{"uniform", "zipf"} {
			b.Run(fmt.Sprintf("objects=%d/dist=%s/state=warm", objects, dist), func(b *testing.B) {
				f := cacheFixtureFor(b, objects)
				runWarm(b, f, dist, q, perParent)
			})
			b.Run(fmt.Sprintf("objects=%d/dist=%s/state=cold", objects, dist), func(b *testing.B) {
				f := cacheFixtureFor(b, objects)
				runCold(b, f, dist, q)
			})
		}
	}
}

func runWarm(b *testing.B, f *cacheFixture, dist string, q cacheQuery, perParent bool) {
	b.Helper()
	idx, err := indexread.Open(f.dir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()
	queries := cacheKeys
	if perParent {
		queries = len(f.parents[dist])
	}
	for i := range queries { // first touch of every page, untimed
		q(b, idx, f, dist, i)
	}
	hw := benchutil.NewHWCounters(b)
	defer hw.Close()
	b.ReportAllocs()
	before := benchutil.ReadPageStats(b)
	b.ResetTimer()
	hw.Start()
	for b.Loop() {
		for i := range queries {
			q(b, idx, f, dist, i)
		}
	}
	hw.Stop()
	b.StopTimer()
	total := b.N * queries
	benchutil.ReadPageStats(b).Sub(before).Report(b, total)
	hw.Report(b, total)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/q")
}

func runCold(b *testing.B, f *cacheFixture, dist string, q cacheQuery) {
	b.Helper()
	var stats benchutil.PageStats
	mapped := map[string]int64{}
	hw := benchutil.NewHWCounters(b)
	defer hw.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		benchutil.DropPageCache(b, f.dir)
		idx, err := indexread.Open(f.dir)
		if err != nil {
			b.Fatal(err)
		}
		before := benchutil.ReadPageStats(b)
		b.StartTimer()
		hw.Start()
		for i := range cacheColdQueries {
			q(b, idx, f, dist, i)
		}
		hw.Stop()
		b.StopTimer()
		d := benchutil.ReadPageStats(b).Sub(before)
		stats.MinorFaults += d.MinorFaults
		stats.MajorFaults += d.MajorFaults
		stats.ReadBytes += d.ReadBytes
		for file, n := range benchutil.MappedBytes(b, f.dir) {
			mapped[fileGroup(file)] += n
		}
		if err := idx.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
	total := b.N * cacheColdQueries
	stats.Report(b, total)
	hw.Report(b, total)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(total), "ns/q")
	for group, n := range mapped {
		b.ReportMetric(float64(n)/1024/float64(total), group+"KiB/q")
	}
}

// fileGroup names the index structure a file belongs to.
func fileGroup(file string) string {
	switch {
	case strings.HasPrefix(file, "tier_stats"):
		return "tiers"
	case strings.HasPrefix(file, "mph"):
		return "mphf"
	case strings.HasPrefix(file, "prefix_dict"):
		return "prefix"
	case strings.HasPrefix(file, "depth"):
		return "depth"
	case strings.HasPrefix(file, "core"):
		return "core"
	default:
		return "other"
	}
}
