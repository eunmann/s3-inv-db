package sqliteagg

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	_ "github.com/mattn/go-sqlite3"
)

// TestProfileAggregationStrategies compares different SQLite write strategies
// to identify the real bottleneck and optimal approach.
func TestProfileAggregationStrategies(t *testing.T) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		t.Skip("set S3INV_LONG_BENCH=1 to run profiling tests")
	}

	sizes := []int{100000, 500000}

	for _, size := range sizes {
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
		objects := gen.Generate()

		t.Run(fmt.Sprintf("objects=%d", size), func(t *testing.T) {
			// Strategy 1: Current UPSERT with TEXT PRIMARY KEY
			t.Run("current_upsert", func(t *testing.T) {
				profileCurrentUpsert(t, objects)
			})

			// Strategy 2: INSERT-only into unindexed staging, aggregate at end
			t.Run("staging_aggregate", func(t *testing.T) {
				profileStagingAggregate(t, objects)
			})

			// Strategy 3: Prefix ID normalization
			t.Run("prefix_id_normalized", func(t *testing.T) {
				profilePrefixIDNormalized(t, objects)
			})

			// Strategy 4: Pure in-memory aggregation, single bulk insert at end
			t.Run("full_memory_aggregate", func(t *testing.T) {
				profileFullMemoryAggregate(t, objects)
			})
		})
	}
}

func profileCurrentUpsert(t *testing.T, objects []benchutil.FakeObject) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "current.db")

	start := time.Now()

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	cfg.BulkWriteMode = true
	agg, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		t.Fatalf("BeginChunk failed: %v", err)
	}

	addStart := time.Now()
	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			t.Fatalf("AddObject failed: %v", err)
		}
	}
	addDuration := time.Since(addStart)

	commitStart := time.Now()
	if err := agg.MarkChunkDone("test-chunk"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	commitDuration := time.Since(commitStart)

	prefixCount, _ := agg.PrefixCount()
	totalDuration := time.Since(start)

	t.Logf("CURRENT UPSERT: objects=%d prefixes=%d", len(objects), prefixCount)
	t.Logf("  AddObject phase: %v (%.0f obj/s)", addDuration, float64(len(objects))/addDuration.Seconds())
	t.Logf("  Commit phase: %v", commitDuration)
	t.Logf("  Total: %v (%.0f obj/s)", totalDuration, float64(len(objects))/totalDuration.Seconds())
}

func profileStagingAggregate(t *testing.T, objects []benchutil.FakeObject) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "staging.db")

	start := time.Now()

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Optimal PRAGMAs for bulk writes
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
	for _, p := range pragmas {
		db.Exec(p)
	}

	// Create UNINDEXED staging table - this is the key difference
	// No PRIMARY KEY, no UNIQUE constraint = no B-tree maintenance during writes
	db.Exec(`CREATE TABLE staging (
		prefix TEXT,
		depth INTEGER,
		count INTEGER,
		bytes INTEGER,
		tier_id INTEGER
	)`)

	// Accumulate deltas in memory first (like current impl)
	deltas := make(map[string]*prefixDelta)
	for _, obj := range objects {
		accumulateDeltaToMap(deltas, "", 0, obj.Size, obj.TierID)
		depth := 1
		for i := 0; i < len(obj.Key); i++ {
			if obj.Key[i] == '/' {
				accumulateDeltaToMap(deltas, obj.Key[:i+1], depth, obj.Size, obj.TierID)
				depth++
			}
		}
	}
	memoryDuration := time.Since(start)

	// Bulk INSERT into staging (no index = very fast)
	insertStart := time.Now()
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO staging (prefix, depth, count, bytes, tier_id) VALUES (?, ?, ?, ?, ?)")

	// For each delta, insert one row per tier that has data
	for prefix, delta := range deltas {
		// Insert total
		stmt.Exec(prefix, delta.depth, delta.totalCount, delta.totalBytes, -1)
		// Insert per-tier
		for tierID := range tiers.NumTiers {
			if delta.tierCounts[tierID] > 0 {
				stmt.Exec(prefix, delta.depth, delta.tierCounts[tierID], delta.tierBytes[tierID], int(tierID))
			}
		}
	}
	stmt.Close()
	tx.Commit()
	insertDuration := time.Since(insertStart)

	// Create final aggregated table with index
	aggregateStart := time.Now()

	// Build tier columns for final table
	var tierCols string
	for i := range tiers.NumTiers {
		tierCols += fmt.Sprintf(", t%d_count INTEGER DEFAULT 0, t%d_bytes INTEGER DEFAULT 0", i, i)
	}

	db.Exec(fmt.Sprintf(`CREATE TABLE prefix_stats (
		prefix TEXT PRIMARY KEY,
		depth INTEGER,
		total_count INTEGER,
		total_bytes INTEGER%s
	)`, tierCols))

	// Build aggregation query
	var tierSums string
	for i := range tiers.NumTiers {
		tierSums += fmt.Sprintf(", SUM(CASE WHEN tier_id = %d THEN count ELSE 0 END)", i)
		tierSums += fmt.Sprintf(", SUM(CASE WHEN tier_id = %d THEN bytes ELSE 0 END)", i)
	}

	aggregateSQL := fmt.Sprintf(`
		INSERT INTO prefix_stats
		SELECT prefix, MAX(depth),
			SUM(CASE WHEN tier_id = -1 THEN count ELSE 0 END),
			SUM(CASE WHEN tier_id = -1 THEN bytes ELSE 0 END)%s
		FROM staging GROUP BY prefix
	`, tierSums)

	db.Exec(aggregateSQL)
	db.Exec("DROP TABLE staging")
	aggregateDuration := time.Since(aggregateStart)

	var prefixCount int
	db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&prefixCount)

	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	totalDuration := time.Since(start)

	t.Logf("STAGING AGGREGATE: objects=%d prefixes=%d", len(objects), prefixCount)
	t.Logf("  Memory accumulation: %v", memoryDuration)
	t.Logf("  Bulk INSERT (no index): %v", insertDuration)
	t.Logf("  Final aggregation: %v", aggregateDuration)
	t.Logf("  Total: %v (%.0f obj/s)", totalDuration, float64(len(objects))/totalDuration.Seconds())
}

func profilePrefixIDNormalized(t *testing.T, objects []benchutil.FakeObject) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "normalized.db")

	start := time.Now()

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

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
	for _, p := range pragmas {
		db.Exec(p)
	}

	// Normalized schema: prefix_id INTEGER instead of prefix TEXT
	db.Exec(`CREATE TABLE prefix_strings (
		id INTEGER PRIMARY KEY,
		prefix TEXT UNIQUE
	)`)

	var tierCols string
	for i := range tiers.NumTiers {
		tierCols += fmt.Sprintf(", t%d_count INTEGER DEFAULT 0, t%d_bytes INTEGER DEFAULT 0", i, i)
	}

	// Key difference: prefix_id INTEGER PRIMARY KEY - much faster B-tree operations
	db.Exec(fmt.Sprintf(`CREATE TABLE prefix_stats (
		prefix_id INTEGER PRIMARY KEY,
		depth INTEGER,
		total_count INTEGER DEFAULT 0,
		total_bytes INTEGER DEFAULT 0%s
	)`, tierCols))

	// Accumulate in memory
	deltas := make(map[string]*prefixDelta)
	for _, obj := range objects {
		accumulateDeltaToMap(deltas, "", 0, obj.Size, obj.TierID)
		depth := 1
		for i := 0; i < len(obj.Key); i++ {
			if obj.Key[i] == '/' {
				accumulateDeltaToMap(deltas, obj.Key[:i+1], depth, obj.Size, obj.TierID)
				depth++
			}
		}
	}
	memoryDuration := time.Since(start)

	// Phase 1: Insert all prefix strings and get IDs
	insertPrefixStart := time.Now()
	tx, _ := db.Begin()

	prefixIDs := make(map[string]int64, len(deltas))
	insertPrefix, _ := tx.Prepare("INSERT OR IGNORE INTO prefix_strings (prefix) VALUES (?)")
	selectID, _ := tx.Prepare("SELECT id FROM prefix_strings WHERE prefix = ?")

	for prefix := range deltas {
		insertPrefix.Exec(prefix)
		var id int64
		selectID.QueryRow(prefix).Scan(&id)
		prefixIDs[prefix] = id
	}
	insertPrefix.Close()
	selectID.Close()
	tx.Commit()
	insertPrefixDuration := time.Since(insertPrefixStart)

	// Phase 2: UPSERT stats using INTEGER key (much faster)
	upsertStart := time.Now()
	tx, _ = db.Begin()

	// Build upsert SQL for stats
	cols := "prefix_id, depth, total_count, total_bytes"
	updates := "total_count = total_count + excluded.total_count, total_bytes = total_bytes + excluded.total_bytes"
	placeholders := "?, ?, ?, ?"
	for i := range tiers.NumTiers {
		cols += fmt.Sprintf(", t%d_count, t%d_bytes", i, i)
		updates += fmt.Sprintf(", t%d_count = t%d_count + excluded.t%d_count", i, i, i)
		updates += fmt.Sprintf(", t%d_bytes = t%d_bytes + excluded.t%d_bytes", i, i, i)
		placeholders += ", ?, ?"
	}

	upsertSQL := fmt.Sprintf(`
		INSERT INTO prefix_stats (%s) VALUES (%s)
		ON CONFLICT(prefix_id) DO UPDATE SET %s
	`, cols, placeholders, updates)

	upsertStmt, _ := tx.Prepare(upsertSQL)

	args := make([]interface{}, 4+int(tiers.NumTiers)*2)
	for prefix, delta := range deltas {
		args[0] = prefixIDs[prefix]
		args[1] = delta.depth
		args[2] = delta.totalCount
		args[3] = delta.totalBytes
		for i := range tiers.NumTiers {
			args[4+int(i)*2] = delta.tierCounts[i]
			args[4+int(i)*2+1] = delta.tierBytes[i]
		}
		upsertStmt.Exec(args...)
	}
	upsertStmt.Close()
	tx.Commit()
	upsertDuration := time.Since(upsertStart)

	var prefixCount int
	db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&prefixCount)

	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	totalDuration := time.Since(start)

	t.Logf("PREFIX ID NORMALIZED: objects=%d prefixes=%d", len(objects), prefixCount)
	t.Logf("  Memory accumulation: %v", memoryDuration)
	t.Logf("  Insert prefix strings: %v", insertPrefixDuration)
	t.Logf("  UPSERT stats (INTEGER key): %v", upsertDuration)
	t.Logf("  Total: %v (%.0f obj/s)", totalDuration, float64(len(objects))/totalDuration.Seconds())
}

func profileFullMemoryAggregate(t *testing.T, objects []benchutil.FakeObject) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "memory.db")

	start := time.Now()

	// Full in-memory aggregation - no SQLite during accumulation
	deltas := make(map[string]*prefixDelta)
	for _, obj := range objects {
		accumulateDeltaToMap(deltas, "", 0, obj.Size, obj.TierID)
		depth := 1
		for i := 0; i < len(obj.Key); i++ {
			if obj.Key[i] == '/' {
				accumulateDeltaToMap(deltas, obj.Key[:i+1], depth, obj.Size, obj.TierID)
				depth++
			}
		}
	}
	memoryDuration := time.Since(start)

	// Single bulk write at the end
	writeStart := time.Now()

	db, _ := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	defer db.Close()

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
	for _, p := range pragmas {
		db.Exec(p)
	}

	var tierCols string
	for i := range tiers.NumTiers {
		tierCols += fmt.Sprintf(", t%d_count INTEGER DEFAULT 0, t%d_bytes INTEGER DEFAULT 0", i, i)
	}

	// Create table WITHOUT index first (faster inserts)
	db.Exec(fmt.Sprintf(`CREATE TABLE prefix_stats (
		prefix TEXT,
		depth INTEGER,
		total_count INTEGER,
		total_bytes INTEGER%s
	)`, tierCols))

	// Bulk INSERT all data (no UPSERT, no index)
	tx, _ := db.Begin()
	placeholders := "?, ?, ?, ?"
	for range tiers.NumTiers {
		placeholders += ", ?, ?"
	}
	insertStmt, _ := tx.Prepare(fmt.Sprintf("INSERT INTO prefix_stats VALUES (%s)", placeholders))

	args := make([]interface{}, 4+int(tiers.NumTiers)*2)
	for prefix, delta := range deltas {
		args[0] = prefix
		args[1] = delta.depth
		args[2] = delta.totalCount
		args[3] = delta.totalBytes
		for i := range tiers.NumTiers {
			args[4+int(i)*2] = delta.tierCounts[i]
			args[4+int(i)*2+1] = delta.tierBytes[i]
		}
		insertStmt.Exec(args...)
	}
	insertStmt.Close()
	tx.Commit()
	insertDuration := time.Since(writeStart)

	// Create index at the end
	indexStart := time.Now()
	db.Exec("CREATE UNIQUE INDEX idx_prefix ON prefix_stats(prefix)")
	indexDuration := time.Since(indexStart)

	var prefixCount int
	db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&prefixCount)

	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	totalDuration := time.Since(start)

	t.Logf("FULL MEMORY AGGREGATE: objects=%d prefixes=%d", len(objects), prefixCount)
	t.Logf("  Memory accumulation: %v", memoryDuration)
	t.Logf("  Bulk INSERT (no index): %v", insertDuration)
	t.Logf("  Create index at end: %v", indexDuration)
	t.Logf("  Total: %v (%.0f obj/s)", totalDuration, float64(len(objects))/totalDuration.Seconds())
}

func accumulateDeltaToMap(m map[string]*prefixDelta, prefix string, depth int, size uint64, tierID tiers.ID) {
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

// BenchmarkMemoryAggregator benchmarks the new MemoryAggregator vs standard Aggregator.
func BenchmarkMemoryAggregator(b *testing.B) {
	sizes := []int{100000, 500000}

	for _, size := range sizes {
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
		objects := gen.Generate()

		b.Run(fmt.Sprintf("objects=%d/standard", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "standard.db")
				b.StartTimer()

				cfg := DefaultConfig(dbPath)
				cfg.Synchronous = "OFF"
				cfg.BulkWriteMode = true
				agg, _ := Open(cfg)

				agg.BeginChunk()
				for _, obj := range objects {
					agg.AddObject(obj.Key, obj.Size, obj.TierID)
				}
				agg.MarkChunkDone("bench")
				agg.Commit()

				b.StopTimer()
				if i == b.N-1 {
					pc, _ := agg.PrefixCount()
					b.Logf("standard: prefixes=%d", pc)
				}
				agg.Close()
			}
		})

		b.Run(fmt.Sprintf("objects=%d/memory", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "memory.db")
				b.StartTimer()

				cfg := DefaultMemoryAggregatorConfig(dbPath)
				agg := NewMemoryAggregator(cfg)

				for _, obj := range objects {
					agg.AddObject(obj.Key, obj.Size, obj.TierID)
				}
				agg.Finalize()

				b.StopTimer()
				if i == b.N-1 {
					b.Logf("memory: prefixes=%d", agg.PrefixCount())
				}
				agg.Clear()
			}
		})
	}
}

// BenchmarkMultiRowInsert compares single-row vs multi-row INSERT for bulk writes.
func BenchmarkMultiRowInsert(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1")
	}

	size := 500000
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
	objects := gen.Generate()

	// Pre-compute deltas
	deltas := make(map[string]*prefixDelta)
	for _, obj := range objects {
		accumulateDeltaToMap(deltas, "", 0, obj.Size, obj.TierID)
		depth := 1
		for i := 0; i < len(obj.Key); i++ {
			if obj.Key[i] == '/' {
				accumulateDeltaToMap(deltas, obj.Key[:i+1], depth, obj.Size, obj.TierID)
				depth++
			}
		}
	}

	// Convert to slice for ordered iteration
	entries := make([]prefixEntry, 0, len(deltas))
	for p, d := range deltas {
		entries = append(entries, prefixEntry{p, d})
	}

	b.Run("single_row", func(b *testing.B) {
		benchmarkInsertBatchSize(b, entries, 1)
	})

	b.Run("batch_256", func(b *testing.B) {
		benchmarkInsertBatchSize(b, entries, 256)
	})

	b.Run("batch_1000", func(b *testing.B) {
		benchmarkInsertBatchSize(b, entries, 1000)
	})

	b.Run("batch_2000", func(b *testing.B) {
		benchmarkInsertBatchSize(b, entries, 2000)
	})
}

type prefixEntry struct {
	prefix string
	delta  *prefixDelta
}

func benchmarkInsertBatchSize(b *testing.B, entries []prefixEntry, batchSize int) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "bench.db")

		db, _ := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
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
		for _, p := range pragmas {
			db.Exec(p)
		}

		var tierCols string
		for j := range tiers.NumTiers {
			tierCols += fmt.Sprintf(", t%d_count INTEGER DEFAULT 0, t%d_bytes INTEGER DEFAULT 0", j, j)
		}

		db.Exec(fmt.Sprintf(`CREATE TABLE prefix_stats (
			prefix TEXT,
			depth INTEGER,
			total_count INTEGER,
			total_bytes INTEGER%s
		)`, tierCols))

		b.StartTimer()

		tx, _ := db.Begin()
		colsPerRow := 4 + int(tiers.NumTiers)*2

		if batchSize == 1 {
			// Single row insert
			placeholders := "?, ?, ?, ?"
			for range tiers.NumTiers {
				placeholders += ", ?, ?"
			}
			stmt, _ := tx.Prepare(fmt.Sprintf("INSERT INTO prefix_stats VALUES (%s)", placeholders))

			args := make([]interface{}, colsPerRow)
			for _, e := range entries {
				args[0] = e.prefix
				args[1] = e.delta.depth
				args[2] = e.delta.totalCount
				args[3] = e.delta.totalBytes
				for k := range tiers.NumTiers {
					args[4+int(k)*2] = e.delta.tierCounts[k]
					args[4+int(k)*2+1] = e.delta.tierBytes[k]
				}
				stmt.Exec(args...)
			}
			stmt.Close()
		} else {
			// Multi-row insert
			oneRow := "(?" + strings.Repeat(", ?", colsPerRow-1) + ")"
			rows := make([]string, batchSize)
			for j := range rows {
				rows[j] = oneRow
			}
			multiSQL := fmt.Sprintf("INSERT INTO prefix_stats VALUES %s", strings.Join(rows, ", "))
			singleSQL := fmt.Sprintf("INSERT INTO prefix_stats VALUES %s", oneRow)

			multiStmt, _ := tx.Prepare(multiSQL)
			singleStmt, _ := tx.Prepare(singleSQL)

			batchArgs := make([]interface{}, batchSize*colsPerRow)
			singleArgs := make([]interface{}, colsPerRow)

			for j := 0; j+batchSize <= len(entries); j += batchSize {
				for k := 0; k < batchSize; k++ {
					e := entries[j+k]
					offset := k * colsPerRow
					batchArgs[offset] = e.prefix
					batchArgs[offset+1] = e.delta.depth
					batchArgs[offset+2] = e.delta.totalCount
					batchArgs[offset+3] = e.delta.totalBytes
					for t := range tiers.NumTiers {
						batchArgs[offset+4+int(t)*2] = e.delta.tierCounts[t]
						batchArgs[offset+4+int(t)*2+1] = e.delta.tierBytes[t]
					}
				}
				multiStmt.Exec(batchArgs...)
			}

			// Remainder
			remainder := len(entries) % batchSize
			if remainder > 0 {
				startIdx := len(entries) - remainder
				for _, e := range entries[startIdx:] {
					singleArgs[0] = e.prefix
					singleArgs[1] = e.delta.depth
					singleArgs[2] = e.delta.totalCount
					singleArgs[3] = e.delta.totalBytes
					for t := range tiers.NumTiers {
						singleArgs[4+int(t)*2] = e.delta.tierCounts[t]
						singleArgs[4+int(t)*2+1] = e.delta.tierBytes[t]
					}
					singleStmt.Exec(singleArgs...)
				}
			}

			multiStmt.Close()
			singleStmt.Close()
		}

		tx.Commit()

		b.StopTimer()

		if i == b.N-1 {
			var count int
			db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&count)
			b.Logf("batch=%d rows=%d", batchSize, count)
		}

		db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		db.Close()
	}
}
