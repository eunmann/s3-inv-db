package indexread_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkGate* are the fixed-size read-path benchmarks `make bench-ab`
// compares between two commits (bench/suite.txt). Each op does enough
// work (milliseconds) to be stable across repeated runs; the fixture is
// one 1M-object S3-realistic, multi-tier index built once per process,
// with a deterministic sample of its prefixes.

const (
	gateObjects = 1_000_000
	gateSample  = 100_000 // prefixes per lookup-style op
	gateParents = 1_000   // parents per children-style op
)

type gateFixture struct {
	idx       *indexread.Index
	prefixes  []string // every (count/gateSample)-th prefix
	positions []uint64 // their positions
	parents   []uint64 // sampled positions that have children
}

var (
	gateOnce sync.Once
	gateFix  *gateFixture
	errGate  error

	benchRootOnce sync.Once
	benchRoot     string
)

// TestMain removes the benchmark fixtures, full 1M-object indexes, once
// the binary's tests and benchmarks are done.
func TestMain(m *testing.M) {
	code := m.Run()
	if gateFix != nil {
		_ = gateFix.idx.Close()
	}
	if benchRoot != "" {
		_ = os.RemoveAll(benchRoot)
	}
	os.Exit(code)
}

// gateTempRoot is the directory every benchmark fixture of this binary
// is built under; TestMain removes it.
func gateTempRoot() string {
	benchRootOnce.Do(func() {
		dir, err := os.MkdirTemp("", "s3inv-gate-")
		if err != nil {
			panic(fmt.Sprintf("mkdir temp: %v", err))
		}
		benchRoot = dir
	})

	return benchRoot
}

func gate(b *testing.B) *gateFixture {
	b.Helper()
	gateOnce.Do(func() { gateFix, errGate = buildGateFixture() })
	if gateFix == nil {
		b.Fatalf("gate fixture: %v", errGate)
	}

	return gateFix
}

func buildGateFixture() (*gateFixture, error) {
	indexDir := filepath.Join(gateTempRoot(), "index")
	if err := buildIndexAt(indexDir, gateObjects); err != nil {
		return nil, err
	}
	idx, err := indexread.Open(indexDir)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	f := &gateFixture{idx: idx}
	// Sample in key order, walked through the API, so the sample is the
	// same prefixes whatever order the index numbers them in.
	keyOrder := keyOrderPositions(idx)
	step := max(uint64(len(keyOrder))/gateSample, 1)
	for i := uint64(0); i < uint64(len(keyOrder)) && len(f.positions) < gateSample; i += step {
		pos := keyOrder[i]
		p, err := idx.PrefixString(pos)
		if err != nil {
			return nil, fmt.Errorf("prefix string: %w", err)
		}
		f.prefixes = append(f.prefixes, p)
		f.positions = append(f.positions, pos)
		if len(f.parents) < gateParents && idx.HasChildren(pos) {
			f.parents = append(f.parents, pos)
		}
	}

	// Visit them in a fixed shuffled order: lookups arrive in no
	// particular order, and key order would favour whichever layout
	// stores prefixes in key order (sequential reads).
	r := rand.New(rand.NewPCG(5, 7))
	r.Shuffle(len(f.prefixes), func(i, j int) {
		f.prefixes[i], f.prefixes[j] = f.prefixes[j], f.prefixes[i]
		f.positions[i], f.positions[j] = f.positions[j], f.positions[i]
	})
	r.Shuffle(len(f.parents), func(i, j int) { f.parents[i], f.parents[j] = f.parents[j], f.parents[i] })

	return f, nil
}

// keyOrderPositions returns every position in key order (the root,
// then each prefix before its children), walking children spans.
func keyOrderPositions(idx *indexread.Index) []uint64 {
	out := make([]uint64, 0, idx.Count())
	stack := []uint64{0}
	for len(stack) > 0 {
		pos := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, pos)
		children, err := idx.DescendantSpan(pos, 1)
		if err != nil {
			panic(fmt.Sprintf("children of %d: %v", pos, err))
		}
		for i := children.Len(); i > 0; i-- {
			stack = append(stack, idx.SpanPosition(children, i-1))
		}
	}

	return out
}

// buildIndexAt builds an index of objects S3-realistic, multi-tier
// objects in indexDir.
func buildIndexAt(indexDir string, objects int) error {
	objs := benchutil.NewGenerator(benchutil.S3RealisticConfig(objects)).Generate()
	agg := extsort.NewAggregator(len(objs), 0)
	for _, o := range objs {
		agg.AddObject(o.Key, o.Size, o.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(indexDir, "")
	if err != nil {
		return fmt.Errorf("new index builder: %w", err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		return fmt.Errorf("set present tiers: %w", err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return fmt.Errorf("add row: %w", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		return fmt.Errorf("finalize index: %w", err)
	}

	return nil
}

// BenchmarkGateStatsForPrefix: string → stats, the /api stats hot path.
func BenchmarkGateStatsForPrefix(b *testing.B) {
	f := gate(b)
	b.ReportAllocs()
	for b.Loop() {
		for _, p := range f.prefixes {
			if _, ok := f.idx.StatsForPrefix(p); !ok {
				b.Fatalf("prefix %q not found", p)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(f.prefixes)), "ns/lookup")
}

// BenchmarkGatePrefixString: position → prefix string reconstruction.
func BenchmarkGatePrefixString(b *testing.B) {
	f := gate(b)
	b.ReportAllocs()
	for b.Loop() {
		for _, pos := range f.positions {
			if _, err := f.idx.PrefixString(pos); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkGateTierBreakdown: per-tier stats for a position.
func BenchmarkGateTierBreakdown(b *testing.B) {
	f := gate(b)
	b.ReportAllocs()
	for b.Loop() {
		for _, pos := range f.positions {
			_ = f.idx.TierBreakdown(pos)
		}
	}
}

// BenchmarkGateChildren: immediate children + top-10 by bytes, the
// browse/top read pattern.
func BenchmarkGateChildren(b *testing.B) {
	f := gate(b)
	b.ReportAllocs()
	for b.Loop() {
		for _, pos := range f.parents {
			if _, err := f.idx.DescendantsAtDepth(pos, 1); err != nil {
				b.Fatal(err)
			}
			if _, err := f.idx.TopByBytes(pos, 1, 10); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkGateTopDeep: the /api/top read pattern far below the root,
// top 25 by bytes over the root's 368k depth-2 descendants and by
// count over its 573k depth-3 ones.
func BenchmarkGateTopDeep(b *testing.B) {
	f := gate(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.idx.TopByBytes(0, 2, 25); err != nil {
			b.Fatal(err)
		}
		if _, err := f.idx.TopByCount(0, 3, 25); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGateDescendantsFiltered: the /api/stats descendants page
// with a size filter: the first 50 of the root's depth-2 descendants
// holding at least 2 GiB, and how many there are.
func BenchmarkGateDescendantsFiltered(b *testing.B) {
	f := gate(b)
	filter := indexread.Filter{MinBytes: 2 << 30}
	b.ReportAllocs()
	for b.Loop() {
		page, err := f.idx.DescendantsPage(0, 2, filter, 0, 50)
		if err != nil {
			b.Fatal(err)
		}
		if page.Total == 0 {
			b.Fatal("no descendants pass the filter")
		}
	}
}
