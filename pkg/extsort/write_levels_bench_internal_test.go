package extsort

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// writeLevelsFixture is a builder whose rows are spooled and planned,
// ready for writeLevels, with what one writeLevels consumes saved to
// restore it from: the spool's depth files, which each run gets hard
// links to, and the core plan, which the child-start tail adds to.
type writeLevelsFixture struct {
	b        *IndexBuilder
	starts   []uint64
	saved    string
	root     string
	corePlan format.CoreStatsPlan
	segments uint32
}

// newWriteLevelsFixture spools the prefixes of objects keys, each in
// one of tiersUsed tiers, as the build's merge would.
func newWriteLevelsFixture(tb testing.TB, objects, tiersUsed int) *writeLevelsFixture {
	tb.Helper()
	agg := NewAggregator(0, 0)
	for i, k := range generateBenchKeys(objects, 4) {
		agg.AddObject(k, uint64(i%4096)<<10, tiers.ID(i%tiersUsed))
	}
	rows := agg.Drain()
	SortPrefixRows(rows)
	root := tb.TempDir()
	b, err := NewIndexBuilder(filepath.Join(root, "out"), root)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(b.cleanup)
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		tb.Fatal(err)
	}
	for _, row := range rows {
		if err := b.Add(row); err != nil {
			tb.Fatal(err)
		}
	}
	segments, err := b.finishSpool()
	if err != nil {
		tb.Fatal(err)
	}
	b.stepProgress = func(string, int64, int64) {}
	f := &writeLevelsFixture{
		b: b, starts: b.levelStarts(), saved: filepath.Join(root, "saved"), root: root,
		corePlan: b.corePlan, segments: segments,
	}
	if err := os.Mkdir(f.saved, format.DirPerm); err != nil {
		tb.Fatal(err)
	}
	for _, sf := range b.spool.depths {
		if err := os.Rename(sf.path, filepath.Join(f.saved, filepath.Base(sf.path))); err != nil {
			tb.Fatal(err)
		}
	}

	return f
}

// reset gives the builder back its spool and core plan, and a new
// empty output directory.
func (f *writeLevelsFixture) reset(tb testing.TB, i int) {
	tb.Helper()
	if err := os.MkdirAll(f.b.spool.dir, format.DirPerm); err != nil {
		tb.Fatal(err)
	}
	for _, sf := range f.b.spool.depths {
		if err := os.Link(filepath.Join(f.saved, filepath.Base(sf.path)), sf.path); err != nil {
			tb.Fatal(err)
		}
	}
	f.b.corePlan = f.corePlan
	f.b.outDir = filepath.Join(f.root, fmt.Sprintf("out-%d", i))
	if err := os.Mkdir(f.b.outDir, format.DirPerm); err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkWriteLevels times writeLevels alone, the replay of about
// 1.27M spooled prefixes into the index files, without the MPHF build
// that runs beside it in Finalize; the spool is built once, outside the
// timer. The busy metrics are the replay's reader and each side's time
// working per op: the largest is the replay's critical path.
func BenchmarkWriteLevels(b *testing.B) {
	const objects = 1_000_000
	for _, tiersUsed := range []int{3, int(tiers.NumTiers)} {
		b.Run(fmt.Sprintf("tiers=%d", tiersUsed), func(b *testing.B) {
			f := newWriteLevelsFixture(b, objects, tiersUsed)
			runtime.GC()
			var sum replayBusy
			n := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				f.reset(b, n)
				b.StartTimer()
				busy, err := f.b.writeLevels(f.starts, f.segments)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := os.RemoveAll(f.b.outDir); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				sum.read += busy.read
				sum.stats += busy.stats
				sum.names += busy.names
				n++
			}
			b.ReportMetric(float64(f.b.posCount), "prefixes")
			b.ReportMetric(float64(sum.read.Nanoseconds())/float64(n), "read-busy-ns/op")
			b.ReportMetric(float64(sum.stats.Nanoseconds())/float64(n), "stats-busy-ns/op")
			b.ReportMetric(float64(sum.names.Nanoseconds())/float64(n), "names-busy-ns/op")
		})
	}
}
