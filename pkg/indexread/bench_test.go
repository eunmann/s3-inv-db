package indexread

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexbuild"
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
var treeSizes = []int{1000, 10000, 100000, 1000000}

// Large tree sizes for specific benchmarks
var largeTreeSizes = []int{1000000, 5000000}

// generateDeepNarrow creates keys with deep paths but few branches
// Shape: a/b/c/.../file1.txt, a/b/c/.../file2.txt, etc.
func generateDeepNarrow(size int) []string {
	keys := make([]string, size)
	depth := 20       // Very deep paths
	numBranches := 26 // a-z
	filesPerLeaf := size / numBranches
	if filesPerLeaf < 1 {
		filesPerLeaf = 1
	}

	idx := 0
	for branch := 0; idx < size && branch < numBranches; branch++ {
		// Build deep path
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
// Shape: prefix0/file.txt, prefix1/file.txt, ..., prefixN/file.txt
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
// Shape: a/a/file.txt, a/b/file.txt, ..., z/z/file.txt
func generateBalanced(size int) []string {
	keys := make([]string, size)
	// Calculate branching factor for ~3 levels
	branchFactor := 26 // a-z
	depth := 3

	idx := 0
	var generate func(prefix string, level int)
	generate = func(prefix string, level int) {
		if idx >= size {
			return
		}
		if level >= depth {
			// Add files at leaf level
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
// Shape: data/2024/01/15/user123/events/event_abc123.json
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
// Shape: root/child000001/, root/child000002/, ..., root/childN/
// This tests querying millions of children at depth 1
func generateWideSingleLevel(size int) []string {
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		// Each key is a unique child prefix with a file
		keys[i] = fmt.Sprintf("root/child%07d/file.txt", i)
	}
	return keys
}

// benchIndex holds a pre-built index for benchmarking
type benchIndex struct {
	idx      *Index
	prefixes []string // all prefix strings in the index
	dir      string
}

// setupBenchIndex creates an index for benchmarking
func setupBenchIndex(b *testing.B, keys []string) *benchIndex {
	b.Helper()

	tmpDir := b.TempDir()
	outDir := filepath.Join(tmpDir, "index")
	sortDir := filepath.Join(tmpDir, "sort")
	if err := os.MkdirAll(sortDir, 0755); err != nil {
		b.Fatalf("MkdirAll failed: %v", err)
	}

	// Create inventory CSV
	var csv strings.Builder
	csv.WriteString("Key,Size\n")
	for i, key := range keys {
		size := (i%1000 + 1) * 100 // Vary sizes
		csv.WriteString(fmt.Sprintf("%s,%d\n", key, size))
	}

	invPath := filepath.Join(tmpDir, "inventory.csv")
	if err := os.WriteFile(invPath, []byte(csv.String()), 0644); err != nil {
		b.Fatalf("WriteFile failed: %v", err)
	}

	// Build index
	cfg := indexbuild.Config{
		OutDir:    outDir,
		TmpDir:    sortDir,
		ChunkSize: 100000,
	}

	if err := indexbuild.Build(context.Background(), cfg, []string{invPath}); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

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

				// Pre-generate random indices for reproducibility
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
					continue // Skip if tree isn't deep enough
				}

				name := fmt.Sprintf("%s/size=%d/depth=%d", shape.name, size, depth)

				b.Run(name+"/from_root", func(b *testing.B) {
					rootPos, _ := bi.idx.Lookup("")
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _ = bi.idx.DescendantsAtDepth(rootPos, depth)
					}
				})

				b.Run(name+"/random_prefix", func(b *testing.B) {
					// Find prefixes that have descendants at the target depth
					var validPrefixes []uint64
					for pos := uint64(0); pos < bi.idx.Count(); pos++ {
						nodeDepth := bi.idx.Depth(pos)
						maxInSubtree := bi.idx.MaxDepthInSubtree(pos)
						if maxInSubtree >= nodeDepth+uint32(depth) {
							validPrefixes = append(validPrefixes, pos)
						}
					}

					if len(validPrefixes) == 0 {
						b.Skip("No valid prefixes for this depth")
					}

					rng := rand.New(rand.NewSource(benchSeed))
					randomIndices := make([]int, b.N)
					for i := range randomIndices {
						randomIndices[i] = rng.Intn(len(validPrefixes))
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						pos := validPrefixes[randomIndices[i]]
						_, _ = bi.idx.DescendantsAtDepth(pos, depth)
					}
				})
			}

			bi.Close()
		}
	}
}

// BenchmarkDescendantsUpToDepth benchmarks cumulative depth queries
func BenchmarkDescendantsUpToDepth(b *testing.B) {
	maxDepths := []int{2, 3, 5}

	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			keys := shape.generate(size)
			bi := setupBenchIndex(b, keys)

			for _, maxDepth := range maxDepths {
				if uint32(maxDepth) > bi.idx.MaxDepth() {
					continue
				}

				name := fmt.Sprintf("%s/size=%d/maxDepth=%d", shape.name, size, maxDepth)

				b.Run(name+"/from_root", func(b *testing.B) {
					rootPos, _ := bi.idx.Lookup("")
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _ = bi.idx.DescendantsUpToDepth(rootPos, maxDepth)
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
		size := 10000 // Fixed size for workload comparison
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
				case op < 50: // 50% - Simple lookup + stats
					prefix := bi.prefixes[rng.Intn(len(bi.prefixes))]
					_, _ = bi.idx.StatsForPrefix(prefix)

				case op < 80: // 30% - Descendants at depth 1
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 1)

				case op < 95: // 15% - Descendants at depth 2
					_, _ = bi.idx.DescendantsAtDepth(rootPos, 2)

				default: // 5% - Deep query (depth 3+)
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

// BenchmarkPrefixRetrieval benchmarks getting prefix strings back
func BenchmarkPrefixRetrieval(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			keys := shape.generate(size)
			bi := setupBenchIndex(b, keys)

			name := fmt.Sprintf("%s/size=%d", shape.name, size)

			b.Run(name+"/sequential", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					pos := uint64(i % len(bi.prefixes))
					_, _ = bi.idx.PrefixString(pos)
				}
			})

			b.Run(name+"/random", func(b *testing.B) {
				rng := rand.New(rand.NewSource(benchSeed))
				randomPos := make([]uint64, b.N)
				for i := range randomPos {
					randomPos[i] = uint64(rng.Intn(len(bi.prefixes)))
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = bi.idx.PrefixString(randomPos[i])
				}
			})

			bi.Close()
		}
	}
}

// BenchmarkMillionsOfChildren benchmarks querying prefixes with millions of children
func BenchmarkMillionsOfChildren(b *testing.B) {
	for _, size := range largeTreeSizes {
		name := fmt.Sprintf("size=%d", size)

		b.Run(name+"/build", func(b *testing.B) {
			// Benchmark just the index building for large trees
			for i := 0; i < b.N; i++ {
				keys := generateWideSingleLevel(size)
				bi := setupBenchIndex(b, keys)
				bi.Close()
			}
		})

		// Setup once for query benchmarks
		keys := generateWideSingleLevel(size)
		bi := setupBenchIndex(b, keys)

		b.Run(name+"/descendants_depth1", func(b *testing.B) {
			// Query all children at depth 1 from root
			rootPos, _ := bi.idx.Lookup("root/")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				descendants, _ := bi.idx.DescendantsAtDepth(rootPos, 1)
				_ = len(descendants)
			}
		})

		b.Run(name+"/iterator_depth1", func(b *testing.B) {
			// Iterate through all children at depth 1
			rootPos, _ := bi.idx.Lookup("root/")
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

		b.Run(name+"/iterator_depth1_with_stats", func(b *testing.B) {
			// Iterate and fetch stats for each child
			rootPos, _ := bi.idx.Lookup("root/")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it, err := bi.idx.NewDescendantIterator(rootPos, 1)
				if err != nil {
					b.Fatal(err)
				}
				for it.Next() {
					pos := it.Pos()
					stats := bi.idx.Stats(pos)
					_ = stats.ObjectCount
					_ = stats.TotalBytes
				}
			}
		})

		b.Run(name+"/random_child_lookup", func(b *testing.B) {
			// Random lookups of children
			rng := rand.New(rand.NewSource(benchSeed))
			prefixes := make([]string, b.N)
			for i := range prefixes {
				childIdx := rng.Intn(size)
				prefixes[i] = fmt.Sprintf("root/child%07d/", childIdx)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = bi.idx.Lookup(prefixes[i])
			}
		})

		bi.Close()
	}
}

// BenchmarkLargeTreeOperations benchmarks operations on million+ node trees
func BenchmarkLargeTreeOperations(b *testing.B) {
	for _, size := range largeTreeSizes {
		keys := generateS3Realistic(size)
		bi := setupBenchIndex(b, keys)

		name := fmt.Sprintf("s3_realistic/size=%d", size)

		b.Run(name+"/lookup_random", func(b *testing.B) {
			rng := rand.New(rand.NewSource(benchSeed))
			indices := make([]int, b.N)
			for i := range indices {
				indices[i] = rng.Intn(len(bi.prefixes))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				prefix := bi.prefixes[indices[i]]
				_, _ = bi.idx.Lookup(prefix)
			}
		})

		b.Run(name+"/stats_random", func(b *testing.B) {
			rng := rand.New(rand.NewSource(benchSeed))
			indices := make([]int, b.N)
			for i := range indices {
				indices[i] = rng.Intn(len(bi.prefixes))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				prefix := bi.prefixes[indices[i]]
				_, _ = bi.idx.StatsForPrefix(prefix)
			}
		})

		b.Run(name+"/prefix_string_random", func(b *testing.B) {
			rng := rand.New(rand.NewSource(benchSeed))
			positions := make([]uint64, b.N)
			for i := range positions {
				positions[i] = uint64(rng.Intn(len(bi.prefixes)))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = bi.idx.PrefixString(positions[i])
			}
		})

		bi.Close()
	}
}
