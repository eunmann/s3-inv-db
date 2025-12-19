package indexread

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Seed for reproducible random access patterns
const benchSeed = 42

// Tree shapes for benchmarking
type treeShape struct {
	name        string
	description string
	generate    func(size int) []string // generates inventory keys
}

var treeShapes = []treeShape{
	{
		name:        "deep_narrow",
		description: "Deep paths with few branches (e.g., a/b/c/d/e/...)",
		generate:    generateDeepNarrow,
	},
	{
		name:        "wide_shallow",
		description: "Many top-level prefixes with shallow depth",
		generate:    generateWideShallow,
	},
	{
		name:        "balanced",
		description: "Balanced tree with moderate depth and width",
		generate:    generateBalanced,
	},
	{
		name:        "s3_realistic",
		description: "Realistic S3 structure (dates, IDs, file types)",
		generate:    generateS3Realistic,
	},
	{
		name:        "wide_single_level",
		description: "Many children under single prefix (tests millions of siblings)",
		generate:    generateWideSingleLevel,
	},
}

// Tree sizes for benchmarking
var treeSizes = []int{1000, 10000, 100000}

// generateDeepNarrow creates keys with deep paths but few branches
func generateDeepNarrow(size int) []string {
	keys := make([]string, size)
	depth := 20
	numBranches := 26
	filesPerLeaf := size / numBranches
	if filesPerLeaf < 1 {
		filesPerLeaf = 1
	}

	idx := 0
	for branch := 0; idx < size && branch < numBranches; branch++ {
		var path strings.Builder
		for d := 0; d < depth; d++ {
			path.WriteString(fmt.Sprintf("%c/", 'a'+byte(branch)))
		}
		prefix := path.String()

		for f := 0; idx < size && f < filesPerLeaf; f++ {
			keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
			idx++
		}
	}
	return keys[:idx]
}

// generateWideShallow creates keys with many top-level prefixes
func generateWideShallow(size int) []string {
	keys := make([]string, size)
	filesPerPrefix := 5
	numPrefixes := size / filesPerPrefix
	if numPrefixes < 1 {
		numPrefixes = 1
	}

	idx := 0
	for p := 0; idx < size && p < numPrefixes; p++ {
		prefix := fmt.Sprintf("prefix%05d/", p)
		for f := 0; idx < size && f < filesPerPrefix; f++ {
			keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
			idx++
		}
	}
	return keys[:idx]
}

// generateBalanced creates a balanced tree structure
func generateBalanced(size int) []string {
	keys := make([]string, size)
	branchFactor := 26
	depth := 3

	idx := 0
	var generate func(prefix string, level int)
	generate = func(prefix string, level int) {
		if idx >= size {
			return
		}
		if level >= depth {
			for f := 0; f < 5 && idx < size; f++ {
				keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
				idx++
			}
			return
		}
		for c := 0; c < branchFactor && idx < size; c++ {
			newPrefix := fmt.Sprintf("%s%c/", prefix, 'a'+byte(c))
			generate(newPrefix, level+1)
		}
	}
	generate("", 0)
	return keys[:idx]
}

// generateS3Realistic creates realistic S3-like paths
func generateS3Realistic(size int) []string {
	keys := make([]string, size)
	rng := rand.New(rand.NewSource(benchSeed))

	prefixes := []string{"data", "logs", "backups", "exports", "uploads"}
	years := []string{"2022", "2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}
	extensions := []string{".json", ".csv", ".parquet", ".txt", ".gz"}

	for i := 0; i < size; i++ {
		prefix := prefixes[rng.Intn(len(prefixes))]
		year := years[rng.Intn(len(years))]
		month := months[rng.Intn(len(months))]
		day := fmt.Sprintf("%02d", rng.Intn(28)+1)
		userID := fmt.Sprintf("user%05d", rng.Intn(1000))
		fileID := fmt.Sprintf("file_%08x", rng.Uint32())
		ext := extensions[rng.Intn(len(extensions))]

		keys[i] = fmt.Sprintf("%s/%s/%s/%s/%s/%s%s", prefix, year, month, day, userID, fileID, ext)
	}
	return keys
}

// generateWideSingleLevel creates many prefixes under a single parent
func generateWideSingleLevel(size int) []string {
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = fmt.Sprintf("root/child%07d/file.txt", i)
	}
	return keys
}

// benchIndex holds a pre-built index for benchmarking
type benchIndex struct {
	idx      *Index
	prefixes []string
	dir      string
}

// setupBenchIndex creates an index for benchmarking using SQLite
func setupBenchIndex(b *testing.B, keys []string) *benchIndex {
	b.Helper()

	tmpDir := b.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Create and populate SQLite database
	cfg := sqliteagg.DefaultConfig(dbPath)
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

	// Build index
	buildCfg := indexbuild.SQLiteConfig{
		OutDir:    outDir,
		DBPath:    dbPath,
		SQLiteCfg: cfg,
	}

	if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	// Clean up SQLite DB
	os.Remove(dbPath)

	// Open index
	idx, err := Open(outDir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// Collect all prefixes for random access benchmarks
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

// BenchmarkLookup benchmarks prefix lookup performance
func BenchmarkLookup(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			name := fmt.Sprintf("%s/size=%d", shape.name, size)
			b.Run(name+"/sequential", func(b *testing.B) {
				keys := shape.generate(size)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[i%len(bi.prefixes)]
					_, _ = bi.idx.Lookup(prefix)
				}
			})

			b.Run(name+"/random", func(b *testing.B) {
				keys := shape.generate(size)
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

// BenchmarkStats benchmarks stats retrieval after lookup
func BenchmarkStats(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			name := fmt.Sprintf("%s/size=%d", shape.name, size)

			b.Run(name+"/sequential", func(b *testing.B) {
				keys := shape.generate(size)
				bi := setupBenchIndex(b, keys)
				defer bi.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prefix := bi.prefixes[i%len(bi.prefixes)]
					_, _ = bi.idx.StatsForPrefix(prefix)
				}
			})

			b.Run(name+"/random", func(b *testing.B) {
				keys := shape.generate(size)
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

// BenchmarkDescendantsAtDepth benchmarks depth-based queries
func BenchmarkDescendantsAtDepth(b *testing.B) {
	depths := []int{1, 2, 3, 5}

	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			keys := shape.generate(size)
			bi := setupBenchIndex(b, keys)

			for _, depth := range depths {
				if uint32(depth) > bi.idx.MaxDepth() {
					continue
				}

				name := fmt.Sprintf("%s/size=%d/depth=%d", shape.name, size, depth)

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

// BenchmarkIterator benchmarks the iterator interface
func BenchmarkIterator(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			keys := shape.generate(size)
			bi := setupBenchIndex(b, keys)

			name := fmt.Sprintf("%s/size=%d", shape.name, size)

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

// BenchmarkMixedWorkload simulates realistic mixed query patterns
func BenchmarkMixedWorkload(b *testing.B) {
	for _, shape := range treeShapes {
		size := 10000
		keys := shape.generate(size)
		bi := setupBenchIndex(b, keys)

		name := fmt.Sprintf("%s/mixed", shape.name)

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
