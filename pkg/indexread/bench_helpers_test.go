package indexread

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
)

// sqliteSetup holds a populated SQLite aggregator for benchmarking.
type sqliteSetup struct {
	Agg         *sqliteagg.Aggregator
	DBPath      string
	PrefixCount uint64
}

// Close cleans up the SQLite setup.
func (s *sqliteSetup) Close() error {
	return s.Agg.Close()
}

// setupSQLiteAggregator creates and populates a SQLite aggregator with realistic data.
func setupSQLiteAggregator(tb testing.TB, numObjects int) *sqliteSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Use MemoryAggregator to create the database
	memAgg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	for _, obj := range objects {
		memAgg.AddObject(obj.Key, obj.Size, obj.TierID)
	}

	if err := memAgg.Finalize(); err != nil {
		tb.Fatalf("Finalize failed: %v", err)
	}

	// Open for reading
	agg, err := sqliteagg.Open(sqliteagg.DefaultConfig(dbPath))
	if err != nil {
		tb.Fatalf("Open SQLite failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()

	return &sqliteSetup{
		Agg:         agg,
		DBPath:      dbPath,
		PrefixCount: prefixCount,
	}
}

// setupSQLiteFromKeys creates a SQLite aggregator from raw keys.
func setupSQLiteFromKeys(tb testing.TB, keys []string) *sqliteSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Use MemoryAggregator to create the database
	memAgg := sqliteagg.NewMemoryAggregator(sqliteagg.DefaultMemoryAggregatorConfig(dbPath))

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100)
		memAgg.AddObject(key, size, 0)
	}

	if err := memAgg.Finalize(); err != nil {
		tb.Fatalf("Finalize failed: %v", err)
	}

	// Open for reading
	agg, err := sqliteagg.Open(sqliteagg.DefaultConfig(dbPath))
	if err != nil {
		tb.Fatalf("Open SQLite failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()

	return &sqliteSetup{
		Agg:         agg,
		DBPath:      dbPath,
		PrefixCount: prefixCount,
	}
}
