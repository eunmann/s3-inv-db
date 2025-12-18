// Package extsort implements external sorting for large datasets.
package extsort

import (
	"github.com/eunmann/s3-inv-db/pkg/inventory"
)

// Config holds configuration for external sorting.
type Config struct {
	// MaxRecordsPerChunk is the maximum number of records to hold in memory.
	MaxRecordsPerChunk int
	// TmpDir is the directory for temporary run files.
	TmpDir string
}

// Iterator provides sorted records from a merge.
type Iterator interface {
	// Next advances to the next record. Returns false when done.
	Next() bool
	// Record returns the current record. Valid after Next() returns true.
	Record() inventory.Record
	// Err returns any error encountered during iteration.
	Err() error
	// Close releases resources.
	Close() error
}
