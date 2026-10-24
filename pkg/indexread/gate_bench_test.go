package indexread_test

import (
	"fmt"
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
	gateDir  string
	gateFix  *gateFixture
	errGate  error
)

// TestMain removes the gate fixture, a full 1M-object index, once the
// binary's tests and benchmarks are done.
func TestMain(m *testing.M) {
	code := m.Run()
	if gateFix != nil {
		_ = gateFix.idx.Close()
	}
	if gateDir != "" {
		_ = os.RemoveAll(gateDir)
	}
	os.Exit(code)
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
	dir, err := os.MkdirTemp("", "s3inv-gate-")
	if err != nil {
		return nil, fmt.Errorf("mkdir temp: %w", err)
	}
	gateDir = dir
	indexDir := filepath.Join(dir, "index")
	objs := benchutil.NewGenerator(benchutil.S3RealisticConfig(gateObjects)).Generate()
	agg := extsort.NewAggregator(len(objs), 0)
	for _, o := range objs {
		agg.AddObject(o.Key, o.Size, o.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(indexDir, "")
	if err != nil {
		return nil, fmt.Errorf("new index builder: %w", err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		return nil, fmt.Errorf("set present tiers: %w", err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return nil, fmt.Errorf("add row: %w", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		return nil, fmt.Errorf("finalize index: %w", err)
	}
	idx, err := indexread.Open(indexDir)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	f := &gateFixture{idx: idx}
	step := max(idx.Count()/gateSample, 1)
	for pos := uint64(0); pos < idx.Count() && len(f.positions) < gateSample; pos += step {
		p, err := idx.PrefixString(pos)
		if err != nil {
			return nil, fmt.Errorf("prefix string: %w", err)
		}
		f.prefixes = append(f.prefixes, p)
		f.positions = append(f.positions, pos)
		if len(f.parents) < gateParents && idx.MaxDepthInSubtree(pos) > idx.Depth(pos) {
			f.parents = append(f.parents, pos)
		}
	}

	return f, nil
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
