// Package sqliteagg provides SQLite-based streaming aggregation for S3 inventory prefixes.
package sqliteagg

import "github.com/eunmann/s3-inv-db/pkg/tiers"

// ChunkAggregator defines the interface for transaction-based aggregators.
// Both Aggregator and NormalizedAggregator implement this interface.
type ChunkAggregator interface {
	// Transaction management
	BeginChunk() error
	Commit() error
	Rollback() error

	// Data ingestion
	AddObject(key string, size uint64, tierID tiers.ID) error

	// Chunk tracking for resumability
	MarkChunkDone(chunkID string) error
	ChunkDone(chunkID string) (bool, error)

	// Query operations
	IteratePrefixes() (*PrefixIterator, error)
	PrefixCount() (uint64, error)
	MaxDepth() (uint32, error)
	PresentTiers() ([]tiers.ID, error)

	// Lifecycle
	Close() error
}

// Verify interface compliance at compile time.
var (
	_ ChunkAggregator = (*Aggregator)(nil)
	_ ChunkAggregator = (*NormalizedAggregator)(nil)
)
