// Package sqliteagg provides SQLite-based streaming aggregation for S3 inventory prefixes.
package sqliteagg

import (
	"database/sql"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	_ "github.com/mattn/go-sqlite3"
)

// Config holds configuration for the SQLite aggregator.
type Config struct {
	// DBPath is the path to the SQLite database file.
	DBPath string
	// Synchronous sets the SQLite synchronous pragma.
	// "NORMAL" is the default (good balance of safety and speed).
	// "OFF" for maximum speed (unsafe on crash).
	// "FULL" for maximum safety.
	Synchronous string
	// MmapSize is the mmap size in bytes (default 256MB).
	MmapSize int64
	// CacheSizeKB is the cache size in KB (default 256MB).
	CacheSizeKB int
	// BulkWriteMode enables optimizations for bulk write workloads.
	// When true:
	//   - locking_mode=EXCLUSIVE (hold lock for entire session)
	//   - wal_autocheckpoint=0 (disable auto-checkpoint, manual at close)
	// This improves write throughput significantly but prevents concurrent readers.
	BulkWriteMode bool

	// ReadOnlyMode enables optimizations for read-only access.
	// When true:
	//   - Uses query_only mode (prevents accidental writes)
	//   - Uses more aggressive mmap and cache settings
	//   - Suitable for building indexes from pre-populated databases
	ReadOnlyMode bool

	// MaxPrefixesPerChunk is a safety limit for per-chunk aggregation.
	// The aggregator accumulates entire chunks in memory before flushing to
	// SQLite. If the number of unique prefixes exceeds this limit, an early
	// flush is triggered to prevent OOM. Set to 0 for no limit (use with caution).
	// Default: 2,000,000 (sufficient for most S3 inventories, ~300MB memory).
	MaxPrefixesPerChunk int
}

// DefaultMaxPrefixesPerChunk is the default safety limit for per-chunk aggregation.
// At ~150 bytes per prefix, 2M prefixes uses ~300MB of memory.
const DefaultMaxPrefixesPerChunk = 2_000_000

// DefaultConfig returns a default configuration tuned for performance.
// Uses per-chunk aggregation by default for maximum throughput.
func DefaultConfig(dbPath string) Config {
	return Config{
		DBPath:              dbPath,
		Synchronous:         "NORMAL",
		MmapSize:            268435456, // 256MB
		CacheSizeKB:         262144,    // 256MB
		MaxPrefixesPerChunk: DefaultMaxPrefixesPerChunk,
	}
}

// ReadOptimizedConfig returns a configuration optimized for read-only access.
// Uses larger mmap and cache sizes for better sequential scan performance.
// Ideal for building indexes from pre-populated databases.
func ReadOptimizedConfig(dbPath string) Config {
	return Config{
		DBPath:       dbPath,
		Synchronous:  "OFF",
		MmapSize:     2147483648, // 2GB - aggressive mmap for read workloads
		CacheSizeKB:  524288,     // 512MB page cache
		ReadOnlyMode: true,
	}
}

// Validate checks configuration values and returns an error for invalid settings.
func (c *Config) Validate() error {
	if c.DBPath == "" {
		return fmt.Errorf("DBPath is required")
	}
	switch c.Synchronous {
	case "", "OFF", "NORMAL", "FULL":
		// Valid values
	default:
		return fmt.Errorf("invalid Synchronous value %q: must be OFF, NORMAL, or FULL", c.Synchronous)
	}
	if c.MmapSize < 0 {
		return fmt.Errorf("MmapSize must be non-negative, got %d", c.MmapSize)
	}
	if c.CacheSizeKB < 0 {
		return fmt.Errorf("CacheSizeKB must be non-negative, got %d", c.CacheSizeKB)
	}
	return nil
}

// SQLite parameter limit and optimal batch sizing.
// SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 32,766 (since SQLite 3.32.0).
const (
	// SQLiteMaxVariables is the maximum number of bound parameters per statement.
	SQLiteMaxVariables = 32766

	// MaxRowsPerBatch is the maximum rows per batched INSERT statement.
	// With sparse schema, both tables have 4 columns per row.
	// 32766 / 4 = 8191 rows per batch.
	MaxRowsPerBatch = SQLiteMaxVariables / 4
)

// Aggregator provides read-only access to a SQLite prefix stats database.
// For writing, use MemoryAggregator which creates the database.
type Aggregator struct {
	db  *sql.DB
	cfg Config
}

// PrefixRow represents a row from the prefix_stats table.
// This struct is designed for efficient scanning with concrete types only.
// All fields use exact types matching SQLite storage (TEXT→string, INTEGER→int/uint64).
type PrefixRow struct {
	Prefix     string
	Depth      int
	TotalCount uint64
	TotalBytes uint64
	TierCounts [tiers.NumTiers]uint64
	TierBytes  [tiers.NumTiers]uint64
}

// scanPrefixBaseStats scans the 4 base columns into a PrefixRow.
// Uses concrete type pointers only - no interface{} or reflection.
// Query must be: SELECT prefix, depth, total_count, total_bytes FROM prefix_stats
func scanPrefixBaseStats(rows *sql.Rows, r *PrefixRow) error {
	return rows.Scan(&r.Prefix, &r.Depth, &r.TotalCount, &r.TotalBytes)
}

// TierRow represents a row from the prefix_tier_stats table.
// Designed for efficient concrete-type scanning.
type TierRow struct {
	Prefix    string
	TierCode  int
	TierCount uint64
	TierBytes uint64
}

// scanTierRow scans the 4 tier columns into a TierRow.
// Uses concrete type pointers only - no interface{} or reflection.
// Query must be: SELECT prefix, tier_code, tier_count, tier_bytes FROM prefix_tier_stats
func scanTierRow(rows *sql.Rows, r *TierRow) error {
	return rows.Scan(&r.Prefix, &r.TierCode, &r.TierCount, &r.TierBytes)
}

// Open creates or opens a SQLite database for aggregation.
func Open(cfg Config) (*Aggregator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	log := logging.WithPhase("sqlite_open")

	db, err := sql.Open("sqlite3", cfg.DBPath+"?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	// Apply PRAGMA settings for performance
	// Order matters: page_size must be set before any tables are created
	pragmas := []string{
		"PRAGMA page_size=32768",                               // Must be first, before tables
		"PRAGMA journal_mode=WAL",                              // Write-ahead logging for concurrency
		fmt.Sprintf("PRAGMA synchronous=%s", cfg.Synchronous),  // NORMAL is good balance
		"PRAGMA temp_store=MEMORY",                             // Temp tables in RAM
		fmt.Sprintf("PRAGMA mmap_size=%d", cfg.MmapSize),       // Memory-mapped I/O
		fmt.Sprintf("PRAGMA cache_size=-%d", cfg.CacheSizeKB),  // Page cache size
		"PRAGMA busy_timeout=10000",                            // 10s timeout to avoid SQLITE_BUSY
	}

	// Bulk write mode: optimizations for heavy write workloads
	if cfg.BulkWriteMode {
		pragmas = append(pragmas,
			"PRAGMA locking_mode=EXCLUSIVE",  // Hold lock for entire session
			"PRAGMA wal_autocheckpoint=0",    // Manual checkpoints only
		)
	}

	// Read-only mode: optimizations for read-only access (index building)
	if cfg.ReadOnlyMode {
		pragmas = append(pragmas,
			"PRAGMA query_only=ON", // Prevent accidental writes
		)
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("execute pragma %q: %w", pragma, err)
		}
	}

	// Create schema only for write mode
	if !cfg.ReadOnlyMode {
		if err := createSchema(db); err != nil {
			db.Close()
			return nil, fmt.Errorf("create schema: %w", err)
		}
	}

	log.Info().
		Str("db_path", cfg.DBPath).
		Str("synchronous", cfg.Synchronous).
		Bool("bulk_write_mode", cfg.BulkWriteMode).
		Bool("read_only_mode", cfg.ReadOnlyMode).
		Msg("opened SQLite aggregator")

	return &Aggregator{
		db:  db,
		cfg: cfg,
	}, nil
}

func createSchema(db *sql.DB) error {
	// Create sparse tier schema:
	// - prefix_stats: base stats only (4 columns)
	// - prefix_tier_stats: tier-specific stats, only for tiers with data
	//
	// Both tables use WITHOUT ROWID for efficient clustered index storage.
	// Sequential ORDER BY prefix scans become straight B-tree walks.
	createSQL := `
		CREATE TABLE IF NOT EXISTS prefix_stats (
			prefix TEXT NOT NULL PRIMARY KEY,
			depth INTEGER NOT NULL,
			total_count INTEGER NOT NULL DEFAULT 0,
			total_bytes INTEGER NOT NULL DEFAULT 0
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS prefix_tier_stats (
			prefix TEXT NOT NULL,
			tier_code INTEGER NOT NULL,
			tier_count INTEGER NOT NULL DEFAULT 0,
			tier_bytes INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (prefix, tier_code)
		) WITHOUT ROWID;
	`

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	return nil
}

// Close closes the database connection.
func (a *Aggregator) Close() error {
	return a.db.Close()
}

// extractPrefixes returns all directory prefixes for a key.
// For "a/b/c.txt", returns ["a/", "a/b/"]
// For "a/b/c/", returns ["a/", "a/b/", "a/b/c/"]
func extractPrefixes(key string) []string {
	// Pre-allocate with capacity 8 - S3 keys rarely have >8 path components
	prefixes := make([]string, 0, 8)
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			prefixes = append(prefixes, key[:i+1])
		}
	}
	return prefixes
}

// PrefixCount returns the total number of prefixes in the database.
func (a *Aggregator) PrefixCount() (uint64, error) {
	var count uint64
	err := a.db.QueryRow("SELECT COUNT(*) FROM prefix_stats").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count prefixes: %w", err)
	}
	return count, nil
}

// MaxDepth returns the maximum depth in the prefix stats.
func (a *Aggregator) MaxDepth() (uint32, error) {
	var maxDepth sql.NullInt64
	err := a.db.QueryRow("SELECT MAX(depth) FROM prefix_stats").Scan(&maxDepth)
	if err != nil {
		return 0, fmt.Errorf("get max depth: %w", err)
	}
	if !maxDepth.Valid {
		return 0, nil
	}
	return uint32(maxDepth.Int64), nil
}

// PresentTiers returns the list of tier IDs that have data in the database.
// Uses the sparse prefix_tier_stats table for efficient lookup.
func (a *Aggregator) PresentTiers() ([]tiers.ID, error) {
	query := "SELECT DISTINCT tier_code FROM prefix_tier_stats ORDER BY tier_code"
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query tiers: %w", err)
	}
	defer rows.Close()

	// Pre-allocate with capacity for all tiers (at most tiers.NumTiers)
	present := make([]tiers.ID, 0, tiers.NumTiers)
	for rows.Next() {
		var tierCode int
		if err := rows.Scan(&tierCode); err != nil {
			return nil, fmt.Errorf("scan tier: %w", err)
		}
		present = append(present, tiers.ID(tierCode))
	}

	return present, rows.Err()
}

// IteratePrefixes returns an iterator over all prefixes in lexicographic order.
// This fetches base stats and loads tier data for present tiers.
func (a *Aggregator) IteratePrefixes() (*PrefixIterator, error) {
	return a.IteratePrefixesForTiers(nil) // nil means load all present tiers
}

// IteratePrefixesForTiers returns an iterator that fetches prefixes and their tier data.
// Uses the sparse schema with two tables:
// - prefix_stats: base stats (4 columns)
// - prefix_tier_stats: tier-specific stats
//
// If presentTiers is nil, tier data is loaded for all tiers with data.
// If presentTiers is empty slice, no tier data is loaded.
// If presentTiers contains specific tiers, only those tiers are loaded.
func (a *Aggregator) IteratePrefixesForTiers(presentTiers []tiers.ID) (*PrefixIterator, error) {
	// If nil, discover present tiers
	if presentTiers == nil {
		var err error
		presentTiers, err = a.PresentTiers()
		if err != nil {
			return nil, fmt.Errorf("get present tiers: %w", err)
		}
	}

	// Load tier data into map for efficient lookup
	tierData := make(map[string][tiers.NumTiers]struct {
		Count uint64
		Bytes uint64
	})

	if len(presentTiers) > 0 {
		query := "SELECT prefix, tier_code, tier_count, tier_bytes FROM prefix_tier_stats ORDER BY prefix, tier_code"
		rows, err := a.db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("query tier stats: %w", err)
		}
		defer rows.Close()

		// Reuse single TierRow struct - no allocations per row
		var tr TierRow
		for rows.Next() {
			// Use type-safe scan helper - concrete types only, no interface{}
			if err := scanTierRow(rows, &tr); err != nil {
				return nil, fmt.Errorf("scan tier row: %w", err)
			}
			entry := tierData[tr.Prefix]
			entry[tr.TierCode].Count = tr.TierCount
			entry[tr.TierCode].Bytes = tr.TierBytes
			tierData[tr.Prefix] = entry
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate tier stats: %w", err)
		}
	}

	// Open prefix stats query
	query := "SELECT prefix, depth, total_count, total_bytes FROM prefix_stats ORDER BY prefix"
	prefixRows, err := a.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query prefix stats: %w", err)
	}

	return &PrefixIterator{
		rows:         prefixRows,
		tierData:     tierData,
		presentTiers: presentTiers,
	}, nil
}

// PrefixIterator iterates over prefix rows in lexicographic order.
// Combines base stats from prefix_stats with tier data from prefix_tier_stats.
type PrefixIterator struct {
	rows         *sql.Rows
	current      PrefixRow
	err          error
	tierData     map[string][tiers.NumTiers]struct{ Count, Bytes uint64 }
	presentTiers []tiers.ID
}

// Next advances to the next row. Returns false when done.
func (it *PrefixIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if !it.rows.Next() {
		it.err = it.rows.Err()
		return false
	}

	// Use type-safe scan helper - concrete types only, no interface{}
	// Scans 4 columns: prefix, depth, total_count, total_bytes
	if err := scanPrefixBaseStats(it.rows, &it.current); err != nil {
		it.err = fmt.Errorf("scan prefix row: %w", err)
		return false
	}

	// Clear and populate tier data from pre-loaded map
	it.current.TierCounts = [tiers.NumTiers]uint64{}
	it.current.TierBytes = [tiers.NumTiers]uint64{}

	if entry, ok := it.tierData[it.current.Prefix]; ok {
		for i := range tiers.NumTiers {
			it.current.TierCounts[i] = entry[i].Count
			it.current.TierBytes[i] = entry[i].Bytes
		}
	}

	return true
}

// Row returns a copy of the current row.
// Use RowPtr() to avoid the 256-byte copy if you only need to read values.
func (it *PrefixIterator) Row() PrefixRow {
	return it.current
}

// RowPtr returns a pointer to the current row (reused across iterations).
// This avoids copying the 256-byte PrefixRow struct on each iteration.
// WARNING: The returned pointer is only valid until the next call to Next().
func (it *PrefixIterator) RowPtr() *PrefixRow {
	return &it.current
}

// Err returns any error encountered during iteration.
func (it *PrefixIterator) Err() error {
	return it.err
}

// Close closes the iterator.
func (it *PrefixIterator) Close() error {
	return it.rows.Close()
}
