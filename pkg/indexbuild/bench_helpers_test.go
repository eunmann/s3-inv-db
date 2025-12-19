package indexbuild

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
)

// SQLiteSetup holds a populated SQLite aggregator for benchmarking.
type SQLiteSetup struct {
	Agg         *sqliteagg.Aggregator
	DBPath      string
	PrefixCount uint64
}

// Close cleans up the SQLite setup.
func (s *SQLiteSetup) Close() error {
	return s.Agg.Close()
}

// setupSQLiteAggregator creates and populates a SQLite aggregator with realistic data.
func setupSQLiteAggregator(tb testing.TB, numObjects int) *SQLiteSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	cfg.BulkWriteMode = true

	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		tb.Fatalf("Open SQLite failed: %v", err)
	}

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	if err := agg.BeginChunk(); err != nil {
		agg.Close()
		tb.Fatalf("BeginChunk failed: %v", err)
	}

	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			agg.Close()
			tb.Fatalf("AddObject failed: %v", err)
		}
	}

	if err := agg.Commit(); err != nil {
		agg.Close()
		tb.Fatalf("Commit failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()

	return &SQLiteSetup{
		Agg:         agg,
		DBPath:      dbPath,
		PrefixCount: prefixCount,
	}
}

// setupSQLiteFromKeys creates a SQLite aggregator from raw keys.
func setupSQLiteFromKeys(tb testing.TB, keys []string) *SQLiteSetup {
	tb.Helper()

	tmpDir := tb.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	cfg.BulkWriteMode = true

	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		tb.Fatalf("Open SQLite failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		agg.Close()
		tb.Fatalf("BeginChunk failed: %v", err)
	}

	for i, key := range keys {
		size := uint64((i%1000 + 1) * 100)
		if err := agg.AddObject(key, size, 0); err != nil {
			agg.Close()
			tb.Fatalf("AddObject failed: %v", err)
		}
	}

	if err := agg.Commit(); err != nil {
		agg.Close()
		tb.Fatalf("Commit failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()

	return &SQLiteSetup{
		Agg:         agg,
		DBPath:      dbPath,
		PrefixCount: prefixCount,
	}
}
