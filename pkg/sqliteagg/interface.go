// Package sqliteagg provides SQLite-based streaming aggregation for S3 inventory prefixes.
package sqliteagg

import "github.com/eunmann/s3-inv-db/pkg/tiers"

// ChunkAggregator defines the interface for transaction-based aggregators.
// Both Aggregator and NormalizedAggregator implement this interface.
//
// Note: Resume support has been removed. All builds are one-shot operations
// optimized for maximum throughput. If a build fails, it should be rerun
// from scratch.
type ChunkAggregator interface {
	// Transaction management
	BeginChunk() error
	Commit() error
	Rollback() error

	// Data ingestion
	AddObject(key string, size uint64, tierID tiers.ID) error

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
