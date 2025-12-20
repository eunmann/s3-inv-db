package indexread

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// indexSetup holds a built index for benchmarking.
type indexSetup struct {
	IndexDir    string
	PrefixCount uint64
}

// setupIndex creates and populates an index with realistic data.
func setupIndex(tb testing.TB, numObjects int) *indexSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	indexDir := filepath.Join(tmpDir, "index")

	// Use extsort aggregator
	agg := extsort.NewAggregator(numObjects, 0)

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}

	// Drain and sort
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	// Build index
	builder, err := extsort.NewIndexBuilder(indexDir)
	if err != nil {
		tb.Fatalf("NewIndexBuilder failed: %v", err)
	}

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			tb.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		tb.Fatalf("Finalize failed: %v", err)
	}

	return &indexSetup{
		IndexDir:    indexDir,
		PrefixCount: builder.Count(),
	}
}

// setupIndexFromKeys creates an index from raw keys.
func setupIndexFromKeys(tb testing.TB, keys []string) *indexSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	indexDir := filepath.Join(tmpDir, "index")

	// Use extsort aggregator
	agg := extsort.NewAggregator(len(keys), 0)

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100)
		agg.AddObject(key, size, tiers.Standard)
	}

	// Drain and sort
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	// Build index
	builder, err := extsort.NewIndexBuilder(indexDir)
	if err != nil {
		tb.Fatalf("NewIndexBuilder failed: %v", err)
	}

	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			tb.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finalize(); err != nil {
		tb.Fatalf("Finalize failed: %v", err)
	}

	return &indexSetup{
		IndexDir:    indexDir,
		PrefixCount: builder.Count(),
	}
}
