package extsort

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/inventory"
)

const benchSeed = 42

// benchReader implements inventory.Reader for benchmarking
type benchReader struct {
	keys []string
	pos  int
}

func (m *benchReader) Read() (inventory.Record, error) {
	if m.pos >= len(m.keys) {
		return inventory.Record{}, io.EOF
	}
	rec := inventory.Record{Key: m.keys[m.pos], Size: uint64((m.pos%1000 + 1) * 100)}
	m.pos++
	return rec, nil
}

func (m *benchReader) Close() error {
	return nil
}

func generateRandomKeys(size int, rng *rand.Rand) []string {
	keys := make([]string, size)
	prefixes := []string{"data", "logs", "backups", "exports", "uploads"}
	years := []string{"2022", "2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}

	for i := 0; i < size; i++ {
		prefix := prefixes[rng.Intn(len(prefixes))]
		year := years[rng.Intn(len(years))]
		month := months[rng.Intn(len(months))]
		day := fmt.Sprintf("%02d", rng.Intn(28)+1)
		userID := fmt.Sprintf("user%05d", rng.Intn(1000))
		fileID := fmt.Sprintf("file_%08x", rng.Uint32())

		keys[i] = fmt.Sprintf("%s/%s/%s/%s/%s/%s.json", prefix, year, month, day, userID, fileID)
	}
	return keys
}

func BenchmarkSorterAddRecords(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	chunkSizes := []int{10000, 100000, 1000000}

	for _, size := range sizes {
		for _, chunkSize := range chunkSizes {
			if chunkSize < size/10 {
				continue // Skip unrealistic combinations
			}

			name := fmt.Sprintf("records=%d/chunk=%d", size, chunkSize)
			rng := rand.New(rand.NewSource(benchSeed))
			keys := generateRandomKeys(size, rng)

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tmpDir := b.TempDir()
					sorter := NewSorter(Config{
						MaxRecordsPerChunk: chunkSize,
						TmpDir:             tmpDir,
					})

					reader := &benchReader{keys: keys}
					if err := sorter.AddRecords(context.Background(), reader); err != nil {
						b.Fatal(err)
					}
					sorter.Cleanup()
				}
			})
		}
	}
}

func BenchmarkSorterMerge(b *testing.B) {
	sizes := []int{10000, 100000}
	numChunks := []int{2, 4, 8}

	for _, size := range sizes {
		for _, chunks := range numChunks {
			chunkSize := size / chunks
			if chunkSize < 100 {
				continue
			}

			name := fmt.Sprintf("records=%d/chunks=%d", size, chunks)
			rng := rand.New(rand.NewSource(benchSeed))
			keys := generateRandomKeys(size, rng)

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tmpDir := b.TempDir()
					sorter := NewSorter(Config{
						MaxRecordsPerChunk: chunkSize,
						TmpDir:             tmpDir,
					})

					reader := &benchReader{keys: keys}
					if err := sorter.AddRecords(context.Background(), reader); err != nil {
						b.Fatal(err)
					}

					b.StartTimer()
					iter, err := sorter.Merge(context.Background())
					if err != nil {
						b.Fatal(err)
					}

					// Consume all records
					count := 0
					for iter.Next() {
						count++
						_ = iter.Record()
					}
					iter.Close()
					b.StopTimer()

					sorter.Cleanup()
				}
			})
		}
	}
}

func BenchmarkSorterFullPipeline(b *testing.B) {
	// Standard sizes - 1M is separate for longer runs
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		name := fmt.Sprintf("records=%d", size)
		rng := rand.New(rand.NewSource(benchSeed))
		keys := generateRandomKeys(size, rng)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tmpDir := b.TempDir()
				sorter := NewSorter(Config{
					MaxRecordsPerChunk: 100000,
					TmpDir:             tmpDir,
				})

				reader := &benchReader{keys: keys}
				if err := sorter.AddRecords(context.Background(), reader); err != nil {
					b.Fatal(err)
				}

				iter, err := sorter.Merge(context.Background())
				if err != nil {
					b.Fatal(err)
				}

				count := 0
				for iter.Next() {
					count++
					_ = iter.Record()
				}
				iter.Close()
				sorter.Cleanup()

				if count != size {
					b.Fatalf("expected %d records, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkSorterFullPipeline_LargeScale tests at 1M scale - run separately
func BenchmarkSorterFullPipeline_LargeScale(b *testing.B) {
	size := 1000000
	rng := rand.New(rand.NewSource(benchSeed))
	keys := generateRandomKeys(size, rng)

	b.Run("records=1000000", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tmpDir := b.TempDir()
			sorter := NewSorter(Config{
				MaxRecordsPerChunk: 100000,
				TmpDir:             tmpDir,
			})

			reader := &benchReader{keys: keys}
			if err := sorter.AddRecords(context.Background(), reader); err != nil {
				b.Fatal(err)
			}

			iter, err := sorter.Merge(context.Background())
			if err != nil {
				b.Fatal(err)
			}

			count := 0
			for iter.Next() {
				count++
				_ = iter.Record()
			}
			iter.Close()
			sorter.Cleanup()

			if count != size {
				b.Fatalf("expected %d records, got %d", size, count)
			}
		}
	})
}
