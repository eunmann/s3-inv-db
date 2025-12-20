// Package sqliteagg provides SQLite-based streaming aggregation for S3 inventory prefixes.
package sqliteagg

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// MemoryAggregatorConfig holds configuration for the memory-based aggregator.
type MemoryAggregatorConfig struct {
	// DBPath is the path to the SQLite database file (for final write).
	DBPath string
	// InitialCapacity is the initial capacity for the prefix map.
	// Set based on expected unique prefix count. Default: 1M.
	InitialCapacity int
	// MultiRowBatchSize is the number of rows per INSERT statement.
	// Larger values reduce SQLite exec calls. Default: 1000.
	MultiRowBatchSize int
}

// DefaultMemoryAggregatorConfig returns default configuration.
func DefaultMemoryAggregatorConfig(dbPath string) MemoryAggregatorConfig {
	return MemoryAggregatorConfig{
		DBPath:            dbPath,
		InitialCapacity:   1_000_000,
		MultiRowBatchSize: MaxRowsPerBatch, // Use optimal batch size from shared constants
	}
}

// PrefixStats holds aggregated stats for a single prefix.
type PrefixStats struct {
	Depth      int
	TotalCount uint64
	TotalBytes uint64
	TierCounts [tiers.NumTiers]uint64
	TierBytes  [tiers.NumTiers]uint64
}

// MemoryAggregator aggregates S3 inventory prefixes entirely in memory,
// then writes to SQLite only at the end. This is optimized for one-shot
// build workflows where resumability is not needed.
//
// Advantages over standard Aggregator:
// - No SQLite I/O during accumulation phase
// - Single bulk write at end (no UPSERT overhead)
// - Deferred index creation (no B-tree maintenance during writes)
// - Multi-row INSERT batching
//
// Limitations:
// - Not resumable (all data lost if process crashes)
// - Memory usage proportional to unique prefix count
type MemoryAggregator struct {
	cfg      MemoryAggregatorConfig
	prefixes map[string]*PrefixStats
	log      zerolog.Logger
}

// NewMemoryAggregator creates a new memory-based aggregator.
func NewMemoryAggregator(cfg MemoryAggregatorConfig) *MemoryAggregator {
	if cfg.InitialCapacity <= 0 {
		cfg.InitialCapacity = 1_000_000
	}
	if cfg.MultiRowBatchSize <= 0 {
		cfg.MultiRowBatchSize = 1000
	}

	return &MemoryAggregator{
		cfg:      cfg,
		prefixes: make(map[string]*PrefixStats, cfg.InitialCapacity),
		log:      logging.WithPhase("memory_aggregator"),
	}
}

// AddObject adds an object's stats to all its prefix ancestors.
func (m *MemoryAggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	// Accumulate root prefix
	m.accumulate("", 0, size, tierID)

	// Accumulate each directory prefix
	depth := 1
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			m.accumulate(key[:i+1], depth, size, tierID)
			depth++
		}
	}
}

func (m *MemoryAggregator) accumulate(prefix string, depth int, size uint64, tierID tiers.ID) {
	stats, ok := m.prefixes[prefix]
	if !ok {
		stats = &PrefixStats{Depth: depth}
		m.prefixes[prefix] = stats
	}
	stats.TotalCount++
	stats.TotalBytes += size
	stats.TierCounts[tierID]++
	stats.TierBytes[tierID] += size
}

// PrefixCount returns the current number of unique prefixes.
func (m *MemoryAggregator) PrefixCount() int {
	return len(m.prefixes)
}

// MemoryUsageEstimate returns an estimate of memory usage in bytes.
func (m *MemoryAggregator) MemoryUsageEstimate() int64 {
	// Rough estimate: map overhead + per-entry overhead
	// Each entry: ~32 bytes key overhead + 30 byte avg key + 88 bytes stats
	return int64(len(m.prefixes)) * 150
}

// Finalize writes all accumulated data to SQLite and creates indexes.
// This is the only SQLite I/O performed by this aggregator.
func (m *MemoryAggregator) Finalize() error {
	start := time.Now()
	prefixCount := len(m.prefixes)

	m.log.Info().
		Int("prefix_count", prefixCount).
		Int64("memory_estimate_mb", m.MemoryUsageEstimate()/(1024*1024)).
		Msg("finalizing memory aggregator to SQLite")

	// Open database
	db, err := sql.Open("sqlite3", m.cfg.DBPath+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()

	// Apply bulk-write optimized PRAGMAs
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
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("pragma %s: %w", p, err)
		}
	}

	// Create table with WITHOUT ROWID for efficient primary key storage.
	// Since we insert prefixes in sorted order, this is efficient and avoids
	// the extra rowid B-tree overhead. The prefix column is the natural primary key.
	var tierCols strings.Builder
	for i := range tiers.NumTiers {
		tierCols.WriteString(fmt.Sprintf(", t%d_count INTEGER NOT NULL, t%d_bytes INTEGER NOT NULL", i, i))
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE prefix_stats (
			prefix TEXT NOT NULL PRIMARY KEY,
			depth INTEGER NOT NULL,
			total_count INTEGER NOT NULL,
			total_bytes INTEGER NOT NULL%s
		) WITHOUT ROWID
	`, tierCols.String())

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Build multi-row INSERT statement using shared constants
	colsPerRow := ColsPerRow
	batchSize := m.cfg.MultiRowBatchSize

	oneRow := "(?" + strings.Repeat(", ?", colsPerRow-1) + ")"
	batchRows := make([]string, batchSize)
	for i := range batchRows {
		batchRows[i] = oneRow
	}
	multiSQL := "INSERT INTO prefix_stats VALUES " + strings.Join(batchRows, ", ")
	singleSQL := "INSERT INTO prefix_stats VALUES " + oneRow

	// Bulk INSERT in single transaction
	insertStart := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	multiStmt, err := tx.Prepare(multiSQL)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare multi-row: %w", err)
	}

	singleStmt, err := tx.Prepare(singleSQL)
	if err != nil {
		multiStmt.Close()
		tx.Rollback()
		return fmt.Errorf("prepare single-row: %w", err)
	}

	// Collect prefixes into slice for batching
	entries := make([]struct {
		prefix string
		stats  *PrefixStats
	}, 0, len(m.prefixes))
	for p, s := range m.prefixes {
		entries = append(entries, struct {
			prefix string
			stats  *PrefixStats
		}{p, s})
	}

	// Sort entries by prefix for improved B-tree locality.
	// Sequential writes reduce page splits and cache misses.
	slices.SortFunc(entries, func(a, b struct {
		prefix string
		stats  *PrefixStats
	}) int {
		return strings.Compare(a.prefix, b.prefix)
	})

	// Reusable args slices
	batchArgs := make([]interface{}, batchSize*colsPerRow)
	singleArgs := make([]interface{}, colsPerRow)

	// Process full batches
	for i := 0; i+batchSize <= len(entries); i += batchSize {
		for j := 0; j < batchSize; j++ {
			e := entries[i+j]
			offset := j * colsPerRow
			batchArgs[offset] = e.prefix
			batchArgs[offset+1] = e.stats.Depth
			batchArgs[offset+2] = e.stats.TotalCount
			batchArgs[offset+3] = e.stats.TotalBytes
			for k := range tiers.NumTiers {
				batchArgs[offset+4+int(k)*2] = e.stats.TierCounts[k]
				batchArgs[offset+4+int(k)*2+1] = e.stats.TierBytes[k]
			}
		}
		if _, err := multiStmt.Exec(batchArgs...); err != nil {
			multiStmt.Close()
			singleStmt.Close()
			tx.Rollback()
			return fmt.Errorf("multi-row insert at %d: %w", i, err)
		}
	}

	// Process remainder
	remainder := len(entries) % batchSize
	if remainder > 0 {
		startIdx := len(entries) - remainder
		for _, e := range entries[startIdx:] {
			singleArgs[0] = e.prefix
			singleArgs[1] = e.stats.Depth
			singleArgs[2] = e.stats.TotalCount
			singleArgs[3] = e.stats.TotalBytes
			for k := range tiers.NumTiers {
				singleArgs[4+int(k)*2] = e.stats.TierCounts[k]
				singleArgs[4+int(k)*2+1] = e.stats.TierBytes[k]
			}
			if _, err := singleStmt.Exec(singleArgs...); err != nil {
				multiStmt.Close()
				singleStmt.Close()
				tx.Rollback()
				return fmt.Errorf("single-row insert: %w", err)
			}
		}
	}

	multiStmt.Close()
	singleStmt.Close()

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	insertDuration := time.Since(insertStart)

	// No separate index needed - WITHOUT ROWID table with PRIMARY KEY provides
	// the index as the clustered table structure itself.
	indexDuration := time.Duration(0)

	// Checkpoint WAL to main database file
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("wal checkpoint: %w", err)
	}

	totalDuration := time.Since(start)

	m.log.Info().
		Int("prefix_count", prefixCount).
		Dur("insert_duration", insertDuration).
		Dur("index_duration", indexDuration).
		Dur("total_duration", totalDuration).
		Float64("rows_per_sec", float64(prefixCount)/insertDuration.Seconds()).
		Msg("finalized to SQLite")

	return nil
}

// Clear releases memory by clearing the prefix map.
func (m *MemoryAggregator) Clear() {
	m.prefixes = nil
}
