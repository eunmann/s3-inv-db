package triebuild

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/inventory"
)

const benchSeed = 42

// mockIterator implements extsort.Iterator for benchmarking
type mockIterator struct {
	records []inventory.Record
	pos     int
}

func (m *mockIterator) Next() bool {
	if m.pos >= len(m.records) {
		return false
	}
	m.pos++
	return true
}

func (m *mockIterator) Record() inventory.Record {
	return m.records[m.pos-1]
}

func (m *mockIterator) Err() error {
	return nil
}

func (m *mockIterator) Close() error {
	return nil
}

// Tree shape generators
func generateDeepNarrowKeys(size int) []string {
	keys := make([]string, size)
	depth := 20
	numBranches := 26
	filesPerLeaf := size / numBranches
	if filesPerLeaf < 1 {
		filesPerLeaf = 1
	}

	idx := 0
	for branch := 0; idx < size && branch < numBranches; branch++ {
		prefix := ""
		for d := 0; d < depth; d++ {
			prefix += fmt.Sprintf("%c/", 'a'+byte(branch))
		}
		for f := 0; idx < size && f < filesPerLeaf; f++ {
			keys[idx] = fmt.Sprintf("%sfile%d.txt", prefix, f)
			idx++
		}
	}
	return keys[:idx]
}

func generateWideShallowKeys(size int) []string {
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

func generateBalancedKeys(size int) []string {
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
			generate(fmt.Sprintf("%s%c/", prefix, 'a'+byte(c)), level+1)
		}
	}
	generate("", 0)
	return keys[:idx]
}

func generateS3RealisticKeys(size int) []string {
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

func generateWideSingleLevelKeys(size int) []string {
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = fmt.Sprintf("root/child%07d/file.txt", i)
	}
	return keys
}

// sortKeys sorts keys lexicographically (required for trie builder)
func sortKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	// Simple insertion sort for benchmark setup (keys are mostly sorted already)
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}
	return sorted
}

func keysToIterator(keys []string) *mockIterator {
	records := make([]inventory.Record, len(keys))
	for i, key := range keys {
		records[i] = inventory.Record{Key: key, Size: uint64((i%1000 + 1) * 100)}
	}
	return &mockIterator{records: records}
}

type treeShape struct {
	name     string
	generate func(size int) []string
}

var treeShapes = []treeShape{
	{"deep_narrow", generateDeepNarrowKeys},
	{"wide_shallow", generateWideShallowKeys},
	{"balanced", generateBalancedKeys},
	{"s3_realistic", generateS3RealisticKeys},
	{"wide_single_level", generateWideSingleLevelKeys},
}

// Standard sizes for quick benchmarks - covers small to medium scale
var treeSizes = []int{1000, 10000, 100000}

func BenchmarkTrieBuild(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			name := fmt.Sprintf("%s/size=%d", shape.name, size)

			// Generate and sort keys once
			keys := sortKeys(shape.generate(size))

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					builder := New()
					iter := keysToIterator(keys)
					_, err := builder.Build(iter)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkTrieBuild_LargeScale tests at 1M scale - run separately with:
//
//	go test -bench=LargeScale -benchtime=1x ./pkg/triebuild/...
func BenchmarkTrieBuild_LargeScale(b *testing.B) {
	// Single 1M test - representative of large scale behavior
	size := 1000000
	keys := sortKeys(generateS3RealisticKeys(size))

	b.Run("s3_realistic/size=1000000", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := New()
			iter := keysToIterator(keys)
			_, err := builder.Build(iter)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExtractPrefixes(b *testing.B) {
	keys := []struct {
		name string
		key  string
	}{
		{"shallow", "a/b/file.txt"},
		{"medium", "data/2024/01/15/user123/file.json"},
		{"deep", "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/file.txt"},
	}

	for _, k := range keys {
		b.Run(k.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = extractPrefixes(k.key)
			}
		})
	}
}
