package sqliteagg

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	_ "github.com/mattn/go-sqlite3"
)

/*
Benchmark Organization:

1. Unified Benchmarks (aggregator_unified_test.go):
   - BenchmarkChunkAggregator: Compares all ChunkAggregator implementations
   - BenchmarkChunkAggregator_Iterate: Iteration performance
   - BenchmarkChunkAggregator_Scaling: Large-scale tests (gated)

2. Specialized Benchmarks (this file):
   - BenchmarkBuildTrieFromSQLite: Trie construction from SQLite
   - BenchmarkMemoryAggregator: In-memory aggregator (different interface)
   - BenchmarkDeltaThreshold: Tuning delta flush threshold
   - BenchmarkMultiRowBatch: Tuning batch sizes
   - BenchmarkStagingTable: UPSERT vs staging table approach
   - BenchmarkTierDistribution: Impact of tier distribution

Run unified comparisons:
  go test -bench='BenchmarkChunkAggregator$' -benchtime=1x ./pkg/sqliteagg/...

Run all benchmarks:
  go test -bench=. -benchtime=1x ./pkg/sqliteagg/...

Run scaling tests:
  S3INV_LONG_BENCH=1 go test -bench=Scaling -benchtime=1x ./pkg/sqliteagg/...
*/

// BenchmarkBuildTrieFromSQLite benchmarks trie building from SQLite.
func BenchmarkBuildTrieFromSQLite(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildTrieFromSQLite(b, size)
		})
	}
}

func benchmarkBuildTrieFromSQLite(b *testing.B, numObjects int) {
	b.Helper()

	// Setup: populate SQLite database once
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}
	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}
	if err := agg.MarkChunkDone("setup-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()
	b.Logf("setup: objects=%d prefixes=%d", numObjects, prefixCount)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := BuildTrieFromSQLite(agg)
		if err != nil {
			b.Fatalf("BuildTrieFromSQLite failed: %v", err)
		}

		if i == b.N-1 {
			b.Logf("result: nodes=%d max_depth=%d tiers=%v",
				len(result.Nodes), result.MaxDepth, result.PresentTiers)
		}
	}

	b.StopTimer()
	agg.Close()
}

// BenchmarkBuildTrieFromSQLite_Scaling runs larger scale tests (gated).
func BenchmarkBuildTrieFromSQLite_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildTrieFromSQLite(b, size)
		})
	}
}

// BenchmarkAddObject benchmarks the per-object insertion cost.
func BenchmarkAddObject(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}

	// Pre-generate keys to avoid measuring key generation
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(b.N))
	objects := gen.Generate()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := objects[i%len(objects)]
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}

	b.StopTimer()

	if err := agg.MarkChunkDone("bench-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
}

// BenchmarkDeltaThreshold benchmarks different delta flush thresholds.
// This helps tune DeltaFlushThreshold for optimal performance.
func BenchmarkDeltaThreshold(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run threshold sweep")
	}

	thresholds := []int{10000, 25000, 50000, 100000, 200000}
	numObjects := 100000

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	for _, threshold := range thresholds {
		b.Run(fmt.Sprintf("threshold=%d", threshold), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "prefix-agg.db")

				cfg := DefaultConfig(dbPath)
				cfg.Synchronous = "OFF"
				agg, err := Open(cfg)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				b.StartTimer()

				if err := agg.BeginChunk(); err != nil {
					b.Fatalf("BeginChunk failed: %v", err)
				}

				flushCount := 0
				for j, obj := range objects {
					if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
						b.Fatalf("AddObject failed: %v", err)
					}
					// Manual flush at threshold
					if (j+1)%threshold == 0 && len(agg.pendingDeltas) > 0 {
						if err := agg.flushPendingDeltas(); err != nil {
							b.Fatalf("flush failed: %v", err)
						}
						flushCount++
					}
				}

				if err := agg.MarkChunkDone("bench-chunk"); err != nil {
					b.Fatalf("MarkChunkDone failed: %v", err)
				}
				if err := agg.Commit(); err != nil {
					b.Fatalf("Commit failed: %v", err)
				}

				b.StopTimer()

				if i == b.N-1 {
					prefixCount, _ := agg.PrefixCount()
					b.Logf("threshold=%d flushes=%d prefixes=%d", threshold, flushCount+1, prefixCount)
				}
				agg.Close()
			}
		})
	}
}

// BenchmarkTierDistribution benchmarks aggregation with different tier distributions.
func BenchmarkTierDistribution(b *testing.B) {
	distributions := []struct {
		name  string
		tiers map[tiers.ID]float64
	}{
		{
			name:  "single_tier",
			tiers: map[tiers.ID]float64{tiers.Standard: 1.0},
		},
		{
			name: "mixed_tiers",
			tiers: map[tiers.ID]float64{
				tiers.Standard:   0.50,
				tiers.StandardIA: 0.20,
				tiers.GlacierIR:  0.15,
				tiers.ITFrequent: 0.10,
				tiers.ITArchive:  0.05,
			},
		},
		{
			name: "all_tiers",
			tiers: map[tiers.ID]float64{
				tiers.Standard:         0.20,
				tiers.StandardIA:       0.10,
				tiers.OneZoneIA:        0.05,
				tiers.GlacierIR:        0.10,
				tiers.GlacierFR:        0.05,
				tiers.DeepArchive:      0.05,
				tiers.ITFrequent:       0.15,
				tiers.ITInfrequent:     0.10,
				tiers.ITArchiveInstant: 0.10,
				tiers.ITArchive:        0.05,
				tiers.ITDeepArchive:    0.05,
			},
		},
	}

	numObjects := 50000

	for _, dist := range distributions {
		b.Run(dist.name, func(b *testing.B) {
			cfg := benchutil.S3RealisticConfig(numObjects)
			cfg.TierDistribution = dist.tiers

			gen := benchutil.NewGenerator(cfg)
			objects := gen.Generate()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "prefix-agg.db")
				aggCfg := DefaultConfig(dbPath)
				aggCfg.Synchronous = "OFF"
				agg, err := Open(aggCfg)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				b.StartTimer()

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

				b.StopTimer()
				agg.Close()
			}
		})
	}
}

// BenchmarkMultiRowBatch benchmarks different multi-row batch sizes.
func BenchmarkMultiRowBatch(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run batch size sweep")
	}

	batchSizes := []int{64, 128, 256, 512, 1024}
	numObjects := 100000

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "prefix-agg.db")

				cfg := DefaultConfig(dbPath)
				cfg.Synchronous = "OFF"
				agg, err := Open(cfg)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				// Override batch size by rebuilding statement
				multiRowSQL := buildMultiRowUpsertSQL(batchSize)
				colsPerRow := 4 + int(tiers.NumTiers)*2

				b.StartTimer()

				tx, err := agg.db.Begin()
				if err != nil {
					b.Fatalf("Begin failed: %v", err)
				}

				singleStmt, _ := tx.Prepare(buildUpsertSQL())
				multiStmt, _ := tx.Prepare(multiRowSQL)

				pendingDeltas := make(map[string]*prefixDelta, DeltaFlushThreshold)

				// Inline delta accumulation
				for _, obj := range objects {
					accumDelta(pendingDeltas, "", 0, obj.Size, obj.TierID)
					depth := 1
					for j := 0; j < len(obj.Key); j++ {
						if obj.Key[j] == '/' {
							accumDelta(pendingDeltas, obj.Key[:j+1], depth, obj.Size, obj.TierID)
							depth++
						}
					}
				}

				// Flush with custom batch size
				prefixes := make([]string, 0, len(pendingDeltas))
				for prefix := range pendingDeltas {
					prefixes = append(prefixes, prefix)
				}

				batchArgs := make([]interface{}, batchSize*colsPerRow)
				singleArgs := make([]interface{}, colsPerRow)

				for j := 0; j+batchSize <= len(prefixes); j += batchSize {
					for k := 0; k < batchSize; k++ {
						prefix := prefixes[j+k]
						delta := pendingDeltas[prefix]
						offset := k * colsPerRow

						batchArgs[offset] = prefix
						batchArgs[offset+1] = delta.depth
						batchArgs[offset+2] = delta.totalCount
						batchArgs[offset+3] = delta.totalBytes

						for t := range tiers.NumTiers {
							batchArgs[offset+4+int(t)*2] = delta.tierCounts[t]
							batchArgs[offset+4+int(t)*2+1] = delta.tierBytes[t]
						}
					}
					if _, err := multiStmt.Exec(batchArgs...); err != nil {
						b.Fatalf("multi-row exec failed: %v", err)
					}
				}

				remainder := len(prefixes) % batchSize
				if remainder > 0 {
					startIdx := len(prefixes) - remainder
					for _, prefix := range prefixes[startIdx:] {
						delta := pendingDeltas[prefix]
						singleArgs[0] = prefix
						singleArgs[1] = delta.depth
						singleArgs[2] = delta.totalCount
						singleArgs[3] = delta.totalBytes
						for t := range tiers.NumTiers {
							singleArgs[4+int(t)*2] = delta.tierCounts[t]
							singleArgs[4+int(t)*2+1] = delta.tierBytes[t]
						}
						if _, err := singleStmt.Exec(singleArgs...); err != nil {
							b.Fatalf("single-row exec failed: %v", err)
						}
					}
				}

				singleStmt.Close()
				multiStmt.Close()
				tx.Commit()

				b.StopTimer()

				if i == b.N-1 {
					prefixCount, _ := agg.PrefixCount()
					b.Logf("batch=%d prefixes=%d", batchSize, prefixCount)
				}
				agg.Close()
			}
		})
	}
}

// accumDelta is a helper for batch size benchmarks.
func accumDelta(m map[string]*prefixDelta, prefix string, depth int, size uint64, tierID tiers.ID) {
	delta, ok := m[prefix]
	if !ok {
		delta = &prefixDelta{depth: depth}
		m[prefix] = delta
	}
	delta.totalCount++
	delta.totalBytes += size
	delta.tierCounts[tierID]++
	delta.tierBytes[tierID] += size
}

// BenchmarkStagingTable compares UPSERT approach vs staging table + merge.
func BenchmarkStagingTable(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
		objects := gen.Generate()

		b.Run(fmt.Sprintf("objects=%d/upsert", size), func(b *testing.B) {
			benchmarkUpsertApproach(b, objects)
		})

		b.Run(fmt.Sprintf("objects=%d/staging", size), func(b *testing.B) {
			benchmarkStagingApproach(b, objects)
		})
	}
}

func benchmarkUpsertApproach(b *testing.B, objects []benchutil.FakeObject) {
	b.Helper()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")
		b.StartTimer()

		cfg := DefaultConfig(dbPath)
		cfg.Synchronous = "OFF"
		cfg.BulkWriteMode = true
		agg, err := Open(cfg)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
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

		b.StopTimer()
		if i == b.N-1 {
			prefixCount, _ := agg.PrefixCount()
			b.Logf("upsert: objects=%d prefixes=%d", len(objects), prefixCount)
		}
		agg.Close()
	}
}

func benchmarkStagingApproach(b *testing.B, objects []benchutil.FakeObject) {
	b.Helper()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "staging.db")
		b.StartTimer()

		db, err := openStagingDB(dbPath)
		if err != nil {
			b.Fatalf("openStagingDB failed: %v", err)
		}

		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("Begin failed: %v", err)
		}

		insertStmt, err := tx.Prepare(buildStagingInsertSQL())
		if err != nil {
			b.Fatalf("Prepare failed: %v", err)
		}

		for _, obj := range objects {
			if _, err := insertStmt.Exec("", 0, 1, obj.Size, int(obj.TierID)); err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
			depth := 1
			for j := 0; j < len(obj.Key); j++ {
				if obj.Key[j] == '/' {
					prefix := obj.Key[:j+1]
					if _, err := insertStmt.Exec(prefix, depth, 1, obj.Size, int(obj.TierID)); err != nil {
						b.Fatalf("Insert failed: %v", err)
					}
					depth++
				}
			}
		}

		insertStmt.Close()
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit staging failed: %v", err)
		}

		if err := mergeStaging(db); err != nil {
			b.Fatalf("mergeStaging failed: %v", err)
		}

		b.StopTimer()

		if i == b.N-1 {
			var prefixCount int
			db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&prefixCount)
			b.Logf("staging: objects=%d prefixes=%d", len(objects), prefixCount)
		}

		db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		db.Close()
	}
}

func openStagingDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return nil, err
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=OFF",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=268435456",
		"PRAGMA page_size=32768",
		"PRAGMA cache_size=-262144",
		"PRAGMA locking_mode=EXCLUSIVE",
		"PRAGMA wal_autocheckpoint=0",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}

	createStaging := `
		CREATE TABLE IF NOT EXISTS staging (
			prefix TEXT,
			depth INTEGER,
			count INTEGER,
			bytes INTEGER,
			tier_id INTEGER
		)
	`

	var tierCols string
	for i := range tiers.NumTiers {
		tierCols += fmt.Sprintf(",\n    t%d_count INTEGER NOT NULL DEFAULT 0", i)
		tierCols += fmt.Sprintf(",\n    t%d_bytes INTEGER NOT NULL DEFAULT 0", i)
	}

	createPrefixStats := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS prefix_stats (
			prefix TEXT PRIMARY KEY,
			depth INTEGER NOT NULL,
			total_count INTEGER NOT NULL DEFAULT 0,
			total_bytes INTEGER NOT NULL DEFAULT 0%s
		)
	`, tierCols)

	if _, err := db.Exec(createStaging); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(createPrefixStats); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func buildStagingInsertSQL() string {
	return "INSERT INTO staging (prefix, depth, count, bytes, tier_id) VALUES (?, ?, ?, ?, ?)"
}

func mergeStaging(db *sql.DB) error {
	var tierSums, tierCols string
	for i := range tiers.NumTiers {
		if i > 0 {
			tierSums += ", "
			tierCols += ", "
		}
		tierSums += fmt.Sprintf("SUM(CASE WHEN tier_id = %d THEN count ELSE 0 END)", i)
		tierSums += fmt.Sprintf(", SUM(CASE WHEN tier_id = %d THEN bytes ELSE 0 END)", i)
		tierCols += fmt.Sprintf("t%d_count, t%d_bytes", i, i)
	}

	mergeSQL := fmt.Sprintf(`
		INSERT INTO prefix_stats (prefix, depth, total_count, total_bytes, %s)
		SELECT prefix, MAX(depth), SUM(count), SUM(bytes), %s
		FROM staging
		GROUP BY prefix
	`, tierCols, tierSums)

	_, err := db.Exec(mergeSQL)
	if err != nil {
		return fmt.Errorf("merge staging: %w", err)
	}

	_, err = db.Exec("DROP TABLE staging")
	return err
}

// Note: BenchmarkMemoryAggregator is defined in profile_test.go
// with a comparison between Standard and Memory aggregators.
