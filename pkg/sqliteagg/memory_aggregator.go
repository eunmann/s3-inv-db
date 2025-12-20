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

// TierStats holds count and bytes for a single tier.
type TierStats struct {
	Count uint64
	Bytes uint64
}

// PrefixStats holds aggregated stats for a single prefix.
// Tier data is stored sparsely - only tiers with actual data are populated.
type PrefixStats struct {
	Depth      int
	TotalCount uint64
	TotalBytes uint64
	Tiers      map[tiers.ID]*TierStats // sparse - only populated for present tiers
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
		stats = &PrefixStats{
			Depth: depth,
			Tiers: make(map[tiers.ID]*TierStats, 2), // small initial capacity
		}
		m.prefixes[prefix] = stats
	}
	stats.TotalCount++
	stats.TotalBytes += size

	// Sparse tier accumulation - only create entry if tier is used
	tierStats, ok := stats.Tiers[tierID]
	if !ok {
		tierStats = &TierStats{}
		stats.Tiers[tierID] = tierStats
	}
	tierStats.Count++
	tierStats.Bytes += size
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
//
// Uses sparse tier schema:
// - prefix_stats: one row per prefix with base stats (4 columns)
// - prefix_tier_stats: one row per (prefix, tier) pair, only for present tiers
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

	// Create sparse schema:
	// - prefix_stats: base stats only (4 columns) - minimal CGO overhead per row
	// - prefix_tier_stats: tier-specific stats, only for tiers with data
	createSQL := `
		CREATE TABLE prefix_stats (
			prefix TEXT NOT NULL PRIMARY KEY,
			depth INTEGER NOT NULL,
			total_count INTEGER NOT NULL,
			total_bytes INTEGER NOT NULL
		) WITHOUT ROWID;

		CREATE TABLE prefix_tier_stats (
			prefix TEXT NOT NULL,
			tier_code INTEGER NOT NULL,
			tier_count INTEGER NOT NULL,
			tier_bytes INTEGER NOT NULL,
			PRIMARY KEY (prefix, tier_code)
		) WITHOUT ROWID;
	`
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	// Collect and sort prefixes for improved B-tree locality
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

	slices.SortFunc(entries, func(a, b struct {
		prefix string
		stats  *PrefixStats
	}) int {
		return strings.Compare(a.prefix, b.prefix)
	})

	// Bulk INSERT in single transaction
	insertStart := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Insert prefix_stats (4 columns per row)
	const prefixCols = 4
	prefixBatchSize := SQLiteMaxVariables / prefixCols // ~8000 rows per batch

	if err := m.insertPrefixStats(tx, entries, prefixBatchSize); err != nil {
		tx.Rollback()
		return fmt.Errorf("insert prefix_stats: %w", err)
	}

	// Insert prefix_tier_stats (4 columns per row: prefix, tier_code, tier_count, tier_bytes)
	const tierCols = 4
	tierBatchSize := SQLiteMaxVariables / tierCols // ~8000 rows per batch

	tierRowCount, err := m.insertTierStats(tx, entries, tierBatchSize)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("insert prefix_tier_stats: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	insertDuration := time.Since(insertStart)

	// Checkpoint WAL to main database file
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("wal checkpoint: %w", err)
	}

	totalDuration := time.Since(start)

	m.log.Info().
		Int("prefix_count", prefixCount).
		Int("tier_row_count", tierRowCount).
		Dur("insert_duration", insertDuration).
		Dur("total_duration", totalDuration).
		Float64("rows_per_sec", float64(prefixCount)/insertDuration.Seconds()).
		Msg("finalized to SQLite")

	return nil
}

// insertPrefixStats inserts base prefix stats using batched INSERT statements.
func (m *MemoryAggregator) insertPrefixStats(tx *sql.Tx, entries []struct {
	prefix string
	stats  *PrefixStats
}, batchSize int) error {
	const colsPerRow = 4
	oneRow := "(?, ?, ?, ?)"

	// Build batch INSERT statement
	batchRows := make([]string, batchSize)
	for i := range batchRows {
		batchRows[i] = oneRow
	}
	multiSQL := "INSERT INTO prefix_stats VALUES " + strings.Join(batchRows, ", ")
	singleSQL := "INSERT INTO prefix_stats VALUES " + oneRow

	multiStmt, err := tx.Prepare(multiSQL)
	if err != nil {
		return fmt.Errorf("prepare multi-row: %w", err)
	}
	defer multiStmt.Close()

	singleStmt, err := tx.Prepare(singleSQL)
	if err != nil {
		return fmt.Errorf("prepare single-row: %w", err)
	}
	defer singleStmt.Close()

	batchArgs := make([]interface{}, batchSize*colsPerRow)

	// Process full batches
	for i := 0; i+batchSize <= len(entries); i += batchSize {
		for j := 0; j < batchSize; j++ {
			e := entries[i+j]
			offset := j * colsPerRow
			batchArgs[offset] = e.prefix
			batchArgs[offset+1] = e.stats.Depth
			batchArgs[offset+2] = e.stats.TotalCount
			batchArgs[offset+3] = e.stats.TotalBytes
		}
		if _, err := multiStmt.Exec(batchArgs...); err != nil {
			return fmt.Errorf("batch insert at %d: %w", i, err)
		}
	}

	// Process remainder
	remainder := len(entries) % batchSize
	if remainder > 0 {
		startIdx := len(entries) - remainder
		for _, e := range entries[startIdx:] {
			if _, err := singleStmt.Exec(e.prefix, e.stats.Depth, e.stats.TotalCount, e.stats.TotalBytes); err != nil {
				return fmt.Errorf("single insert: %w", err)
			}
		}
	}

	return nil
}

// insertTierStats inserts tier stats using batched INSERT statements.
// Returns the number of tier rows inserted.
func (m *MemoryAggregator) insertTierStats(tx *sql.Tx, entries []struct {
	prefix string
	stats  *PrefixStats
}, batchSize int) (int, error) {
	const colsPerRow = 4
	oneRow := "(?, ?, ?, ?)"

	// Build batch INSERT statement
	batchRows := make([]string, batchSize)
	for i := range batchRows {
		batchRows[i] = oneRow
	}
	multiSQL := "INSERT INTO prefix_tier_stats VALUES " + strings.Join(batchRows, ", ")
	singleSQL := "INSERT INTO prefix_tier_stats VALUES " + oneRow

	multiStmt, err := tx.Prepare(multiSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare multi-row: %w", err)
	}
	defer multiStmt.Close()

	singleStmt, err := tx.Prepare(singleSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare single-row: %w", err)
	}
	defer singleStmt.Close()

	batchArgs := make([]interface{}, batchSize*colsPerRow)

	// Collect all tier rows in sorted order (prefix, tier_code)
	type tierRow struct {
		prefix   string
		tierCode tiers.ID
		count    uint64
		bytes    uint64
	}

	// Count total tier rows for pre-allocation
	totalTierRows := 0
	for _, e := range entries {
		totalTierRows += len(e.stats.Tiers)
	}

	tierRows := make([]tierRow, 0, totalTierRows)
	for _, e := range entries {
		// Collect tiers for this prefix in tier_code order
		for tierCode, ts := range e.stats.Tiers {
			tierRows = append(tierRows, tierRow{
				prefix:   e.prefix,
				tierCode: tierCode,
				count:    ts.Count,
				bytes:    ts.Bytes,
			})
		}
	}

	// Sort by (prefix, tier_code) for optimal B-tree insertion
	slices.SortFunc(tierRows, func(a, b tierRow) int {
		if cmp := strings.Compare(a.prefix, b.prefix); cmp != 0 {
			return cmp
		}
		return int(a.tierCode) - int(b.tierCode)
	})

	// Process full batches
	for i := 0; i+batchSize <= len(tierRows); i += batchSize {
		for j := 0; j < batchSize; j++ {
			tr := tierRows[i+j]
			offset := j * colsPerRow
			batchArgs[offset] = tr.prefix
			batchArgs[offset+1] = int(tr.tierCode)
			batchArgs[offset+2] = tr.count
			batchArgs[offset+3] = tr.bytes
		}
		if _, err := multiStmt.Exec(batchArgs...); err != nil {
			return 0, fmt.Errorf("batch insert at %d: %w", i, err)
		}
	}

	// Process remainder
	remainder := len(tierRows) % batchSize
	if remainder > 0 {
		startIdx := len(tierRows) - remainder
		for _, tr := range tierRows[startIdx:] {
			if _, err := singleStmt.Exec(tr.prefix, int(tr.tierCode), tr.count, tr.bytes); err != nil {
				return 0, fmt.Errorf("single insert: %w", err)
			}
		}
	}

	return len(tierRows), nil
}

// Clear releases memory by clearing the prefix map.
func (m *MemoryAggregator) Clear() {
	m.prefixes = nil
}
