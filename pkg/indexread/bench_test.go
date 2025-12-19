package indexread

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
Benchmark Categories for Index Reading:

1. BenchmarkIndexOpen - Tests index loading from disk
   - Measures: time to mmap files and setup in-memory structures
   - Critical for cold-start performance

2. BenchmarkLookup - Tests single-prefix lookups (MPHF + verify)
   - Measures: ns/op for prefix→position resolution
   - Tests sequential vs random access patterns

3. BenchmarkStats - Tests stats retrieval (lookup + array access)
   - Measures: ns/op for full prefix→stats path

4. BenchmarkTierBreakdown - Tests per-tier statistics retrieval
   - Measures: ns/op for tier data reads

5. BenchmarkDescendantsAtDepth - Tests depth-limited queries
   - Measures: performance of depth index binary search + iteration

6. BenchmarkIterator - Tests iterator interface
   - Measures: per-element iteration cost

7. BenchmarkMixedWorkload - Simulates realistic query mix
   - 50% single-prefix stats, 30% depth-1 queries, 20% deeper queries
*/

// Seed for reproducible random access patterns
const benchSeed = 42

// Tree shapes and sizes for benchmarking
var (
	benchShapes = []string{"deep_narrow", "wide_shallow", "balanced", "s3_realistic", "wide_single_level"}
	benchSizes  = []int{1000, 10000, 100000}
)

// benchIndex holds a pre-built index for benchmarking
type benchIndex struct {
	idx      *Index
	prefixes []string
	dir      string
}

// setupBenchIndex creates an index for benchmarking using SQLite.
func setupBenchIndex(b *testing.B, keys []string) *benchIndex {
	b.Helper()

	tmpDir := b.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF" // Faster for benchmarks
	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		b.Fatalf("Open SQLite failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100)
		if err := agg.AddObject(key, size, tiers.Standard); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}

	if err := agg.MarkChunkDone("bench-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
	if err := agg.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}

	buildCfg := indexbuild.SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: cfg,
	}

	if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	os.Remove(dbPath)

	idx, err := Open(outDir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	prefixes := make([]string, 0, idx.Count())
	for i := uint64(0); i < idx.Count(); i++ {
		p, err := idx.PrefixString(i)
		if err != nil {
			b.Fatalf("PrefixString failed: %v", err)
		}
		prefixes = append(prefixes, p)
	}

	return &benchIndex{
		idx:      idx,
		prefixes: prefixes,
		dir:      outDir,
	}
}

// setupBenchIndexWithTiers creates an index with mixed tier data.
func setupBenchIndexWithTiers(b *testing.B, numObjects int) *benchIndex {
	b.Helper()

	tmpDir := b.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		b.Fatalf("Open SQLite failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}

	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}

	if err := agg.MarkChunkDone("bench-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
	if err := agg.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}

	buildCfg := indexbuild.SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: cfg,
	}

	if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	os.Remove(dbPath)

	idx, err := Open(outDir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	prefixes := make([]string, 0, idx.Count())
	for i := uint64(0); i < idx.Count(); i++ {
		p, err := idx.PrefixString(i)
		if err != nil {
			b.Fatalf("PrefixString failed: %v", err)
		}
		prefixes = append(prefixes, p)
	}

	return &benchIndex{
		idx:      idx,
		prefixes: prefixes,
		dir:      outDir,
	}
}

func (bi *benchIndex) Close() {
	if bi.idx != nil {
		bi.idx.Close()
	}
}

// Shared fixture for index load benchmarks
var (
	fixtureOnce sync.Once
	fixtureDir  string
)

func setupFixtureIndex(b *testing.B) string {
	b.Helper()

	fixtureOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "bench-fixture-*")
		if err != nil {
			b.Fatalf("MkdirTemp failed: %v", err)
		}

		outDir := filepath.Join(tmpDir, "index")
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")

		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(50000))
		objects := gen.Generate()

		cfg := sqliteagg.DefaultConfig(dbPath)
		cfg.Synchronous = "OFF"
		agg, err := sqliteagg.Open(cfg)
		if err != nil {
			b.Fatalf("Open SQLite failed: %v", err)
		}

		if err := agg.BeginChunk(); err != nil {
			b.Fatalf("BeginChunk failed: %v", err)
		}
		for _, obj := range objects {
			if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
				b.Fatalf("AddObject failed: %v", err)
			}
		}
		if err := agg.MarkChunkDone("fixture-chunk"); err != nil {
			b.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
		agg.Close()

		buildCfg := indexbuild.SQLiteConfig{
			OutDir:    outDir,
			DBPath:    dbPath,
			SQLiteCfg: cfg,
		}
		if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
			b.Fatalf("Build failed: %v", err)
		}

		os.Remove(dbPath)
		fixtureDir = outDir
	})

	return fixtureDir
}

// BenchmarkIndexOpen benchmarks the cost of opening an index from disk.
func BenchmarkIndexOpen(b *testing.B) {
	dir := setupFixtureIndex(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx, err := Open(dir)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}

		if i == b.N-1 {
			b.Logf("count=%d max_depth=%d has_tiers=%v",
				idx.Count(), idx.MaxDepth(), idx.HasTierData())
		}

		idx.Close()
	}
}

// BenchmarkLookup benchmarks prefix lookup performance.
func BenchmarkLookup(b *testing.B) {
	for _, shape := range benchShapes {
		for _, size := range benchSizes {
			name := fmt.Sprintf("%s/size=%d", shape, size)

			b.Run(name+"/sequential", func(b *testing.B) {
				keys := benchutil.GenerateKeys(size, shape)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[i%len(bi.prefixes)]
					_, _ = bi.idx.Lookup(prefix)
				}
			})

			b.Run(name+"/random", func(b *testing.B) {
				keys := benchutil.GenerateKeys(size, shape)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				rng := rand.New(rand.NewSource(benchSeed))
				randomIndices := make([]int, b.N)
				for i := range randomIndices {
					randomIndices[i] = rng.Intn(len(bi.prefixes))
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[randomIndices[i]]
					_, _ = bi.idx.Lookup(prefix)
				}
			})
		}
	}
}

// BenchmarkStats benchmarks stats retrieval after lookup.
func BenchmarkStats(b *testing.B) {
	for _, shape := range benchShapes {
		for _, size := range benchSizes {
			name := fmt.Sprintf("%s/size=%d", shape, size)

			b.Run(name+"/sequential", func(b *testing.B) {
				keys := benchutil.GenerateKeys(size, shape)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[i%len(bi.prefixes)]
					_, _ = bi.idx.StatsForPrefix(prefix)
				}
			})

			b.Run(name+"/random", func(b *testing.B) {
				keys := benchutil.GenerateKeys(size, shape)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				rng := rand.New(rand.NewSource(benchSeed))
				randomIndices := make([]int, b.N)
				for i := range randomIndices {
					randomIndices[i] = rng.Intn(len(bi.prefixes))
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[randomIndices[i]]
					_, _ = bi.idx.StatsForPrefix(prefix)
				}
			})
		}
	}
}

// BenchmarkTierBreakdown benchmarks per-tier statistics retrieval.
func BenchmarkTierBreakdown(b *testing.B) {
	sizes := []int{10000, 50000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d/sequential", size), func(b *testing.B) {
			bi := setupBenchIndexWithTiers(b, size)
			defer bi.Close()

			if !bi.idx.HasTierData() {
				b.Skip("index has no tier data")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pos := uint64(i % len(bi.prefixes))
				_ = bi.idx.TierBreakdown(pos)
			}
		})

		b.Run(fmt.Sprintf("objects=%d/random", size), func(b *testing.B) {
			bi := setupBenchIndexWithTiers(b, size)
			defer bi.Close()

			if !bi.idx.HasTierData() {
				b.Skip("index has no tier data")
			}

			rng := rand.New(rand.NewSource(benchSeed))
			randomPos := make([]uint64, b.N)
			for i := range randomPos {
				randomPos[i] = uint64(rng.Intn(len(bi.prefixes)))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = bi.idx.TierBreakdown(randomPos[i])
			}
		})

		b.Run(fmt.Sprintf("objects=%d/all_tiers", size), func(b *testing.B) {
			bi := setupBenchIndexWithTiers(b, size)
			defer bi.Close()

			if !bi.idx.HasTierData() {
				b.Skip("index has no tier data")
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pos := uint64(i % len(bi.prefixes))
				_ = bi.idx.TierBreakdownAll(pos)
			}
		})
	}
}

// BenchmarkDescendantsAtDepth benchmarks depth-based queries.
func BenchmarkDescendantsAtDepth(b *testing.B) {
	depths := []int{1, 2, 3, 5}

	for _, shape := range benchShapes {
		for _, size := range benchSizes {
			keys := benchutil.GenerateKeys(size, shape)
			bi := setupBenchIndex(b, keys)

			for _, depth := range depths {
				if uint32(depth) > bi.idx.MaxDepth() {
					continue
				}

				name := fmt.Sprintf("%s/size=%d/depth=%d", shape, size, depth)

				b.Run(name+"/from_root", func(b *testing.B) {
					rootPos, _ := bi.idx.Lookup("")
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _ = bi.idx.DescendantsAtDepth(rootPos, depth)
					}
				})
			}

			bi.Close()
		}
	}
}

// BenchmarkDescendantsSubtree benchmarks descendants queries on different subtree sizes.
func BenchmarkDescendantsSubtree(b *testing.B) {
	size := 50000
	keys := benchutil.GenerateKeys(size, "s3_realistic")
	bi := setupBenchIndex(b, keys)
	defer bi.Close()

	// Find prefixes with different subtree sizes
	var smallSubtreePrefix, largeSubtreePrefix string
	var smallCount, largeCount int

	for _, p := range bi.prefixes {
		pos, ok := bi.idx.Lookup(p)
		if !ok {
			continue
		}
		subtreeSize := bi.idx.SubtreeEnd(pos) - pos
		if subtreeSize > 5 && subtreeSize < 50 && smallSubtreePrefix == "" {
			smallSubtreePrefix = p
			smallCount = int(subtreeSize)
		}
		if subtreeSize > 500 && largeSubtreePrefix == "" {
			largeSubtreePrefix = p
			largeCount = int(subtreeSize)
		}
		if smallSubtreePrefix != "" && largeSubtreePrefix != "" {
			break
		}
	}

	b.Run("small_subtree", func(b *testing.B) {
		if smallSubtreePrefix == "" {
			b.Skip("no suitable small subtree found")
		}
		pos, _ := bi.idx.Lookup(smallSubtreePrefix)
		b.Logf("prefix=%q subtree_size=%d", smallSubtreePrefix, smallCount)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = bi.idx.DescendantsAtDepth(pos, 1)
		}
	})

	b.Run("large_subtree", func(b *testing.B) {
		if largeSubtreePrefix == "" {
			b.Skip("no suitable large subtree found")
		}
		pos, _ := bi.idx.Lookup(largeSubtreePrefix)
		b.Logf("prefix=%q subtree_size=%d", largeSubtreePrefix, largeCount)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = bi.idx.DescendantsAtDepth(pos, 1)
		}
	})
}

// BenchmarkIterator benchmarks the iterator interface.
func BenchmarkIterator(b *testing.B) {
	for _, shape := range benchShapes {
		for _, size := range benchSizes {
			keys := benchutil.GenerateKeys(size, shape)
			bi := setupBenchIndex(b, keys)

			name := fmt.Sprintf("%s/size=%d", shape, size)

			b.Run(name+"/depth1_iterate_all", func(b *testing.B) {
				rootPos, _ := bi.idx.Lookup("")
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it, err := bi.idx.NewDescendantIterator(rootPos, 1)
					if err != nil {
						b.Fatal(err)
					}
					count := 0
					for it.Next() {
						count++
						_ = it.Pos()
					}
				}
			})

			bi.Close()
		}
	}
}

// BenchmarkMixedWorkload simulates realistic mixed query patterns.
func BenchmarkMixedWorkload(b *testing.B) {
	for _, shape := range benchShapes {
		size := 10000
		keys := benchutil.GenerateKeys(size, shape)
		bi := setupBenchIndex(b, keys)

		name := fmt.Sprintf("%s/mixed", shape)

		b.Run(name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(benchSeed))
			rootPos, _ := bi.idx.Lookup("")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op := rng.Intn(100)
				switch {
				case op < 50:
					prefix := bi.prefixes[rng.Intn(len(bi.prefixes))]
					_, _ = bi.idx.StatsForPrefix(prefix)
				case op < 80:
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 1)
				case op < 95:
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 2)
				default:
					depth := rng.Intn(3) + 3
					if uint32(depth) <= bi.idx.MaxDepth() {
						_, _ = bi.idx.DescendantsAtDepth(rootPos, depth)
					}
				}
			}
		})

		bi.Close()
	}
}

// BenchmarkMixedWorkloadWithTiers includes tier breakdown queries.
func BenchmarkMixedWorkloadWithTiers(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	if !bi.idx.HasTierData() {
		b.Skip("index has no tier data")
	}

	rng := rand.New(rand.NewSource(benchSeed))
	rootPos, _ := bi.idx.Lookup("")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)
		switch {
		case op < 40:
			prefix := bi.prefixes[rng.Intn(len(bi.prefixes))]
			_, _ = bi.idx.StatsForPrefix(prefix)
		case op < 55:
			pos := uint64(rng.Intn(len(bi.prefixes)))
			_ = bi.idx.TierBreakdown(pos)
		case op < 75:
			_, _ = bi.idx.DescendantsAtDepth(rootPos, 1)
		case op < 90:
			_, _ = bi.idx.DescendantsAtDepth(rootPos, 2)
		default:
			depth := rng.Intn(3) + 3
			if uint32(depth) <= bi.idx.MaxDepth() {
				_, _ = bi.idx.DescendantsAtDepth(rootPos, depth)
			}
		}
	}
}

// BenchmarkPrefixHeavy benchmarks many back-to-back prefix queries.
func BenchmarkPrefixHeavy(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 100000)
	defer bi.Close()

	rng := rand.New(rand.NewSource(benchSeed))

	// Pre-generate random indices
	randomIndices := make([]int, b.N)
	for i := range randomIndices {
		randomIndices[i] = rng.Intn(len(bi.prefixes))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefix := bi.prefixes[randomIndices[i]]
		pos, ok := bi.idx.Lookup(prefix)
		if ok {
			_ = bi.idx.Stats(pos)
		}
	}
}

// BenchmarkIndexOpen_Scaling runs larger scale load tests (gated).
func BenchmarkIndexOpen_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			bi := setupBenchIndexWithTiers(b, size)
			bi.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, err := Open(bi.dir)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				idx.Close()
			}
		})
	}
}

// =============================================================================
// Concurrent Query Benchmarks
// =============================================================================
// These benchmarks test query performance under concurrent load using
// b.RunParallel. This simulates multi-goroutine access patterns.

// BenchmarkConcurrentLookup tests concurrent prefix lookup performance.
func BenchmarkConcurrentLookup(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	prefixCount := len(bi.prefixes)

	b.Run("parallel_random", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				idx := rng.Intn(prefixCount)
				_, _ = bi.idx.Lookup(bi.prefixes[idx])
			}
		})
	})

	b.Run("parallel_sequential", func(b *testing.B) {
		var counter uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Use atomic add to get unique sequential indices
				i := int(atomic.AddUint64(&counter, 1) % uint64(prefixCount))
				_, _ = bi.idx.Lookup(bi.prefixes[i])
			}
		})
	})
}

// BenchmarkConcurrentStats tests concurrent stats retrieval performance.
func BenchmarkConcurrentStats(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	prefixCount := len(bi.prefixes)

	b.Run("parallel_random", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				idx := rng.Intn(prefixCount)
				_, _ = bi.idx.StatsForPrefix(bi.prefixes[idx])
			}
		})
	})

	b.Run("parallel_sequential", func(b *testing.B) {
		var counter uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := int(atomic.AddUint64(&counter, 1) % uint64(prefixCount))
				_, _ = bi.idx.StatsForPrefix(bi.prefixes[i])
			}
		})
	})
}

// BenchmarkConcurrentTierBreakdown tests concurrent tier data retrieval.
func BenchmarkConcurrentTierBreakdown(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	if !bi.idx.HasTierData() {
		b.Skip("index has no tier data")
	}

	prefixCount := uint64(len(bi.prefixes))

	b.Run("parallel_random", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				pos := uint64(rng.Int63()) % prefixCount
				_ = bi.idx.TierBreakdown(pos)
			}
		})
	})

	b.Run("parallel_all_tiers", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				pos := uint64(rng.Int63()) % prefixCount
				_ = bi.idx.TierBreakdownAll(pos)
			}
		})
	})
}

// BenchmarkConcurrentDescendants tests concurrent depth-based queries.
func BenchmarkConcurrentDescendants(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	rootPos, _ := bi.idx.Lookup("")
	prefixCount := len(bi.prefixes)

	b.Run("parallel_depth1_root", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = bi.idx.DescendantsAtDepth(rootPos, 1)
			}
		})
	})

	b.Run("parallel_depth2_root", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = bi.idx.DescendantsAtDepth(rootPos, 2)
			}
		})
	})

	b.Run("parallel_random_prefix", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				idx := rng.Intn(prefixCount)
				pos, ok := bi.idx.Lookup(bi.prefixes[idx])
				if ok {
					_, _ = bi.idx.DescendantsAtDepth(pos, 1)
				}
			}
		})
	})
}

// BenchmarkConcurrentMixedWorkload simulates concurrent realistic query patterns.
func BenchmarkConcurrentMixedWorkload(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	hasTiers := bi.idx.HasTierData()
	rootPos, _ := bi.idx.Lookup("")
	prefixCount := len(bi.prefixes)
	maxDepth := bi.idx.MaxDepth()

	b.Run("parallel_mixed", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				op := rng.Intn(100)
				switch {
				case op < 35:
					// Stats lookup (35%)
					prefix := bi.prefixes[rng.Intn(prefixCount)]
					_, _ = bi.idx.StatsForPrefix(prefix)
				case op < 50 && hasTiers:
					// Tier breakdown (15%)
					pos := uint64(rng.Intn(prefixCount))
					_ = bi.idx.TierBreakdown(pos)
				case op < 70:
					// Depth-1 descendants (20%)
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 1)
				case op < 85:
					// Depth-2 descendants (15%)
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 2)
				default:
					// Deeper descendants (15%)
					depth := rng.Intn(3) + 3
					if uint32(depth) <= maxDepth {
						_, _ = bi.idx.DescendantsAtDepth(rootPos, depth)
					}
				}
			}
		})
	})
}

// BenchmarkConcurrentContention tests high-contention scenarios.
func BenchmarkConcurrentContention(b *testing.B) {
	bi := setupBenchIndexWithTiers(b, 50000)
	defer bi.Close()

	// Find a prefix that all goroutines will query (hot spot)
	hotPrefix := bi.prefixes[0]
	hotPos, _ := bi.idx.Lookup(hotPrefix)

	b.Run("parallel_same_prefix", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = bi.idx.Lookup(hotPrefix)
			}
		})
	})

	b.Run("parallel_same_stats", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = bi.idx.Stats(hotPos)
			}
		})
	})

	b.Run("parallel_same_descendants", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = bi.idx.DescendantsAtDepth(hotPos, 1)
			}
		})
	})
}
