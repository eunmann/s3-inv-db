// Package sqliteagg provides SQLite-based streaming aggregation for S3 inventory prefixes.
package sqliteagg

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
}

// DefaultConfig returns a default configuration tuned for performance.
func DefaultConfig(dbPath string) Config {
	return Config{
		DBPath:      dbPath,
		Synchronous: "NORMAL",
		MmapSize:    268435456, // 256MB
		CacheSizeKB: 262144,    // 256MB
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

// prefixDelta accumulates count and byte deltas for a single prefix.
type prefixDelta struct {
	depth      int
	totalCount uint64
	totalBytes uint64
	tierCounts [tiers.NumTiers]uint64
	tierBytes  [tiers.NumTiers]uint64
}

// DeltaFlushThreshold is the number of pending deltas before automatic flush.
// This balances memory usage vs SQLite call frequency.
const DeltaFlushThreshold = 50000

// MultiRowBatchSize is the number of rows per multi-row UPSERT statement.
// Larger batches reduce SQLite exec calls but increase SQL parsing overhead.
// Tuned via BenchmarkMultiRowBatch for optimal performance.
const MultiRowBatchSize = 256

// Aggregator aggregates S3 inventory prefixes into a SQLite database.
type Aggregator struct {
	db          *sql.DB
	cfg         Config
	tierMapping *tiers.Mapping

	// Prepared statements (created per-transaction)
	upsertStmt      *sql.Stmt // Single-row upsert (for remainder)
	multiRowStmt    *sql.Stmt // Multi-row upsert (batch of MultiRowBatchSize)
	markDoneStmt    *sql.Stmt
	colsPerRow      int // Number of columns per row in upsert

	// Current transaction
	tx *sql.Tx

	// In-memory delta accumulation (reduces SQLite calls by ~10x)
	pendingDeltas map[string]*prefixDelta

	// Reusable buffers for flush operations (avoids allocation per flush)
	batchArgs  []interface{}
	singleArgs []interface{}

	// writeMu serializes write transactions for parallel streaming.
	// Multiple goroutines can read (ChunkDone), but writes are serialized.
	writeMu sync.Mutex
}

// PrefixRow represents a row from the prefix_stats table.
type PrefixRow struct {
	Prefix     string
	Depth      int
	TotalCount uint64
	TotalBytes uint64
	TierCounts [tiers.NumTiers]uint64
	TierBytes  [tiers.NumTiers]uint64
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
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		fmt.Sprintf("PRAGMA synchronous=%s", cfg.Synchronous),
		"PRAGMA temp_store=MEMORY",
		fmt.Sprintf("PRAGMA mmap_size=%d", cfg.MmapSize),
		"PRAGMA page_size=32768",
		fmt.Sprintf("PRAGMA cache_size=-%d", cfg.CacheSizeKB),
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("execute pragma %q: %w", pragma, err)
		}
	}

	// Create schema
	if err := createSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	log.Info().
		Str("db_path", cfg.DBPath).
		Str("synchronous", cfg.Synchronous).
		Msg("opened SQLite aggregator")

	return &Aggregator{
		db:          db,
		cfg:         cfg,
		tierMapping: tiers.NewMapping(),
	}, nil
}

func createSchema(db *sql.DB) error {
	// Build prefix_stats table with tier columns
	var tierCols strings.Builder
	for i := range tiers.NumTiers {
		tierCols.WriteString(fmt.Sprintf(",\n    t%d_count INTEGER NOT NULL DEFAULT 0", i))
		tierCols.WriteString(fmt.Sprintf(",\n    t%d_bytes INTEGER NOT NULL DEFAULT 0", i))
	}

	createPrefixStats := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS prefix_stats (
			prefix TEXT PRIMARY KEY,
			depth INTEGER NOT NULL,
			total_count INTEGER NOT NULL DEFAULT 0,
			total_bytes INTEGER NOT NULL DEFAULT 0%s
		)
	`, tierCols.String())

	createChunksDone := `
		CREATE TABLE IF NOT EXISTS chunks_done (
			chunk_id TEXT PRIMARY KEY,
			processed_at TEXT NOT NULL
		)
	`

	if _, err := db.Exec(createPrefixStats); err != nil {
		return fmt.Errorf("create prefix_stats table: %w", err)
	}

	if _, err := db.Exec(createChunksDone); err != nil {
		return fmt.Errorf("create chunks_done table: %w", err)
	}

	return nil
}

// Close closes the database connection.
// If a transaction is in progress, it is rolled back.
func (a *Aggregator) Close() error {
	if a.tx != nil {
		// Rollback error is intentionally ignored during Close.
		// The transaction will be aborted when the connection closes anyway.
		_ = a.tx.Rollback()
		a.tx = nil
	}
	// Close any open prepared statements
	if a.upsertStmt != nil {
		_ = a.upsertStmt.Close()
		a.upsertStmt = nil
	}
	if a.multiRowStmt != nil {
		_ = a.multiRowStmt.Close()
		a.multiRowStmt = nil
	}
	if a.markDoneStmt != nil {
		_ = a.markDoneStmt.Close()
		a.markDoneStmt = nil
	}
	return a.db.Close()
}

// ChunkDone returns true if the chunk has already been processed.
func (a *Aggregator) ChunkDone(chunkID string) (bool, error) {
	var exists int
	err := a.db.QueryRow("SELECT 1 FROM chunks_done WHERE chunk_id = ?", chunkID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check chunk done: %w", err)
	}
	return true, nil
}

// BeginChunk starts a new transaction for processing a chunk.
func (a *Aggregator) BeginChunk() error {
	if a.tx != nil {
		return fmt.Errorf("transaction already in progress")
	}

	tx, err := a.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	a.tx = tx

	// Initialize in-memory delta accumulation
	// Pre-size map to reduce growth overhead (typical ratio: ~2.5 prefixes per object)
	a.pendingDeltas = make(map[string]*prefixDelta, DeltaFlushThreshold)

	// Prepare single-row upsert statement for prefix_stats (used for remainder)
	upsertSQL := buildUpsertSQL()
	a.upsertStmt, err = tx.Prepare(upsertSQL)
	if err != nil {
		tx.Rollback()
		a.tx = nil
		return fmt.Errorf("prepare upsert statement: %w", err)
	}

	// Prepare multi-row upsert statement for batched inserts
	multiRowSQL := buildMultiRowUpsertSQL(MultiRowBatchSize)
	a.multiRowStmt, err = tx.Prepare(multiRowSQL)
	if err != nil {
		a.upsertStmt.Close()
		tx.Rollback()
		a.tx = nil
		return fmt.Errorf("prepare multi-row upsert statement: %w", err)
	}

	// Calculate columns per row for arg slicing
	a.colsPerRow = 4 + int(tiers.NumTiers)*2

	// Initialize reusable buffers for flush operations
	a.batchArgs = make([]interface{}, MultiRowBatchSize*a.colsPerRow)
	a.singleArgs = make([]interface{}, a.colsPerRow)

	// Prepare mark done statement
	a.markDoneStmt, err = tx.Prepare("INSERT INTO chunks_done (chunk_id, processed_at) VALUES (?, ?)")
	if err != nil {
		a.upsertStmt.Close()
		a.multiRowStmt.Close()
		tx.Rollback()
		a.tx = nil
		return fmt.Errorf("prepare mark done statement: %w", err)
	}

	return nil
}

// buildUpsertSQL builds a single-row upsert statement.
func buildUpsertSQL() string {
	return buildMultiRowUpsertSQL(1)
}

// buildMultiRowUpsertSQL builds a multi-row upsert statement for n rows.
func buildMultiRowUpsertSQL(n int) string {
	// Build column list
	cols := []string{"prefix", "depth", "total_count", "total_bytes"}
	updates := []string{
		"total_count = total_count + excluded.total_count",
		"total_bytes = total_bytes + excluded.total_bytes",
	}

	for i := range tiers.NumTiers {
		cols = append(cols, fmt.Sprintf("t%d_count", i), fmt.Sprintf("t%d_bytes", i))
		updates = append(updates,
			fmt.Sprintf("t%d_count = t%d_count + excluded.t%d_count", i, i, i),
			fmt.Sprintf("t%d_bytes = t%d_bytes + excluded.t%d_bytes", i, i, i),
		)
	}

	// Build placeholder for one row
	oneRowPlaceholders := make([]string, len(cols))
	for i := range oneRowPlaceholders {
		oneRowPlaceholders[i] = "?"
	}
	oneRow := "(" + strings.Join(oneRowPlaceholders, ", ") + ")"

	// Build VALUES clause with n rows
	rows := make([]string, n)
	for i := range rows {
		rows[i] = oneRow
	}

	return fmt.Sprintf(`
		INSERT INTO prefix_stats (%s)
		VALUES %s
		ON CONFLICT(prefix) DO UPDATE SET %s
	`, strings.Join(cols, ", "), strings.Join(rows, ", "), strings.Join(updates, ", "))
}

// AddObject adds an object's stats to all its prefix ancestors.
// This should be called for each object in the inventory.
// Deltas are accumulated in memory and flushed to SQLite when threshold is exceeded.
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) error {
	if a.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	// Accumulate root prefix (empty string)
	a.accumulateDelta("", 0, size, tierID)

	// Accumulate each directory prefix
	depth := 1
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			prefix := key[:i+1]
			a.accumulateDelta(prefix, depth, size, tierID)
			depth++
		}
	}

	// Flush if we've accumulated too many unique prefixes
	if len(a.pendingDeltas) >= DeltaFlushThreshold {
		if err := a.flushPendingDeltas(); err != nil {
			return err
		}
	}

	return nil
}

// accumulateDelta adds a delta to the in-memory accumulator.
func (a *Aggregator) accumulateDelta(prefix string, depth int, size uint64, tierID tiers.ID) {
	delta, ok := a.pendingDeltas[prefix]
	if !ok {
		delta = &prefixDelta{depth: depth}
		a.pendingDeltas[prefix] = delta
	}
	delta.totalCount++
	delta.totalBytes += size
	delta.tierCounts[tierID]++
	delta.tierBytes[tierID] += size
}

// flushPendingDeltas writes accumulated deltas to SQLite using multi-row batching.
func (a *Aggregator) flushPendingDeltas() error {
	if len(a.pendingDeltas) == 0 {
		return nil
	}

	// Collect prefixes into a slice for batching
	prefixes := make([]string, 0, len(a.pendingDeltas))
	for prefix := range a.pendingDeltas {
		prefixes = append(prefixes, prefix)
	}

	// Process full batches using reusable batchArgs buffer
	for i := 0; i+MultiRowBatchSize <= len(prefixes); i += MultiRowBatchSize {
		// Fill batch args
		for j := 0; j < MultiRowBatchSize; j++ {
			prefix := prefixes[i+j]
			delta := a.pendingDeltas[prefix]
			offset := j * a.colsPerRow

			a.batchArgs[offset] = prefix
			a.batchArgs[offset+1] = delta.depth
			a.batchArgs[offset+2] = delta.totalCount
			a.batchArgs[offset+3] = delta.totalBytes

			for k := range tiers.NumTiers {
				a.batchArgs[offset+4+int(k)*2] = delta.tierCounts[k]
				a.batchArgs[offset+4+int(k)*2+1] = delta.tierBytes[k]
			}
		}

		if _, err := a.multiRowStmt.Exec(a.batchArgs...); err != nil {
			return fmt.Errorf("multi-row upsert batch at %d: %w", i, err)
		}
	}

	// Process remainder with single-row upserts using reusable singleArgs buffer
	remainder := len(prefixes) % MultiRowBatchSize
	if remainder > 0 {
		startIdx := len(prefixes) - remainder
		for _, prefix := range prefixes[startIdx:] {
			delta := a.pendingDeltas[prefix]

			a.singleArgs[0] = prefix
			a.singleArgs[1] = delta.depth
			a.singleArgs[2] = delta.totalCount
			a.singleArgs[3] = delta.totalBytes

			for k := range tiers.NumTiers {
				a.singleArgs[4+int(k)*2] = delta.tierCounts[k]
				a.singleArgs[4+int(k)*2+1] = delta.tierBytes[k]
			}

			if _, err := a.upsertStmt.Exec(a.singleArgs...); err != nil {
				return fmt.Errorf("upsert prefix %q: %w", prefix, err)
			}
		}
	}

	// Clear the map by creating a new one (faster than deleting keys)
	// Pre-size based on previous batch size
	a.pendingDeltas = make(map[string]*prefixDelta, DeltaFlushThreshold)
	return nil
}

// extractPrefixes returns all directory prefixes for a key.
// For "a/b/c.txt", returns ["a/", "a/b/"]
// For "a/b/c/", returns ["a/", "a/b/", "a/b/c/"]
//
// Note: This is used by pipeline.go and tests. The hot path in AddObject
// uses an inline version to avoid slice allocation per object.
func extractPrefixes(key string) []string {
	var prefixes []string
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			prefixes = append(prefixes, key[:i+1])
		}
	}
	return prefixes
}

// MarkChunkDone marks a chunk as processed within the current transaction.
func (a *Aggregator) MarkChunkDone(chunkID string) error {
	if a.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	_, err := a.markDoneStmt.Exec(chunkID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("mark chunk done: %w", err)
	}
	return nil
}

// Commit commits the current transaction.
// Flushes any remaining pending deltas before committing.
func (a *Aggregator) Commit() error {
	if a.tx == nil {
		return fmt.Errorf("no transaction in progress")
	}

	// Flush any remaining pending deltas
	if err := a.flushPendingDeltas(); err != nil {
		return fmt.Errorf("flush pending deltas: %w", err)
	}
	a.pendingDeltas = nil

	// Close prepared statements (errors intentionally ignored - best effort cleanup)
	if a.upsertStmt != nil {
		_ = a.upsertStmt.Close()
		a.upsertStmt = nil
	}
	if a.multiRowStmt != nil {
		_ = a.multiRowStmt.Close()
		a.multiRowStmt = nil
	}
	if a.markDoneStmt != nil {
		_ = a.markDoneStmt.Close()
		a.markDoneStmt = nil
	}

	err := a.tx.Commit()
	a.tx = nil
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// Rollback rolls back the current transaction.
// Discards any pending in-memory deltas.
func (a *Aggregator) Rollback() error {
	if a.tx == nil {
		return nil
	}

	// Discard pending deltas
	a.pendingDeltas = nil

	// Close prepared statements (errors intentionally ignored - best effort cleanup)
	if a.upsertStmt != nil {
		_ = a.upsertStmt.Close()
		a.upsertStmt = nil
	}
	if a.multiRowStmt != nil {
		_ = a.multiRowStmt.Close()
		a.multiRowStmt = nil
	}
	if a.markDoneStmt != nil {
		_ = a.markDoneStmt.Close()
		a.markDoneStmt = nil
	}

	err := a.tx.Rollback()
	a.tx = nil
	return err
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

// ChunkCount returns the number of processed chunks.
func (a *Aggregator) ChunkCount() (int, error) {
	var count int
	err := a.db.QueryRow("SELECT COUNT(*) FROM chunks_done").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count chunks: %w", err)
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

// PresentTiers returns the list of tiers that have data.
func (a *Aggregator) PresentTiers() ([]tiers.ID, error) {
	var present []tiers.ID

	for i := range tiers.NumTiers {
		var count sql.NullInt64
		query := fmt.Sprintf("SELECT SUM(t%d_count) FROM prefix_stats WHERE depth = 0", i)
		err := a.db.QueryRow(query).Scan(&count)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("check tier %d: %w", i, err)
		}
		if count.Valid && count.Int64 > 0 {
			present = append(present, i)
		}
	}

	return present, nil
}

// IteratePrefixes returns an iterator over all prefixes in lexicographic order.
func (a *Aggregator) IteratePrefixes() (*PrefixIterator, error) {
	// Build SELECT statement with all tier columns
	cols := []string{"prefix", "depth", "total_count", "total_bytes"}
	for i := range tiers.NumTiers {
		cols = append(cols, fmt.Sprintf("t%d_count", i), fmt.Sprintf("t%d_bytes", i))
	}

	query := fmt.Sprintf("SELECT %s FROM prefix_stats ORDER BY prefix", strings.Join(cols, ", "))
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query prefixes: %w", err)
	}

	return &PrefixIterator{rows: rows}, nil
}

// PrefixIterator iterates over prefix rows in lexicographic order.
type PrefixIterator struct {
	rows     *sql.Rows
	current  PrefixRow
	err      error
	scanDest []interface{} // Reused across rows to avoid allocation
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

	// Initialize scan destinations once (lazy init to avoid allocation if iterator never used)
	if it.scanDest == nil {
		it.scanDest = make([]interface{}, 4+int(tiers.NumTiers)*2)
		it.scanDest[0] = &it.current.Prefix
		it.scanDest[1] = &it.current.Depth
		it.scanDest[2] = &it.current.TotalCount
		it.scanDest[3] = &it.current.TotalBytes
		for i := range tiers.NumTiers {
			it.scanDest[4+int(i)*2] = &it.current.TierCounts[i]
			it.scanDest[4+int(i)*2+1] = &it.current.TierBytes[i]
		}
	}

	if err := it.rows.Scan(it.scanDest...); err != nil {
		it.err = fmt.Errorf("scan prefix row: %w", err)
		return false
	}

	return true
}

// Row returns the current row.
func (it *PrefixIterator) Row() PrefixRow {
	return it.current
}

// Err returns any error encountered during iteration.
func (it *PrefixIterator) Err() error {
	return it.err
}

// Close closes the iterator.
func (it *PrefixIterator) Close() error {
	return it.rows.Close()
}
