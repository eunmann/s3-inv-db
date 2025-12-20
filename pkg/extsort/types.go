// Package extsort provides a pure-Go external sort backend for building S3 inventory indexes.
//
// The package implements a chunked prefix aggregation pipeline with external merge sort:
//  1. Stream S3 inventory rows (CSV or Parquet) and aggregate per-prefix statistics in bounded memory.
//  2. When memory threshold is reached, sort and flush to temporary run files.
//  3. K-way merge all run files to produce a globally sorted, aggregated stream.
//  4. Build the final index (columnar arrays + MPHF) in a single streaming pass.
//
// This package does not use CGO or SQLite, making it suitable for pure-Go deployments.
package extsort

import (
	"runtime"
	"slices"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// MaxTiers is the maximum number of storage tiers supported.
// This matches tiers.NumTiers and is used for fixed-size arrays to avoid map allocations.
const MaxTiers = int(tiers.NumTiers)

// PrefixRow represents a single prefix with its aggregated statistics.
// This is the primary data type passed through the external sort pipeline.
//
// The struct uses fixed-size arrays for tier data to avoid map allocations
// in hot paths. Tier index i corresponds to tiers.ID(i).
type PrefixRow struct {
	// Prefix is the full prefix string (e.g., "data/2024/01/").
	Prefix string

	// Depth is the directory depth (number of '/' characters).
	// Uses uint16 to save memory (max depth of 65535 is more than sufficient).
	Depth uint16

	// Count is the total number of objects under this prefix.
	Count uint64

	// TotalBytes is the total size in bytes of all objects under this prefix.
	TotalBytes uint64

	// TierCounts holds object counts per storage tier.
	// Index i corresponds to tiers.ID(i).
	TierCounts [MaxTiers]uint64

	// TierBytes holds byte counts per storage tier.
	// Index i corresponds to tiers.ID(i).
	TierBytes [MaxTiers]uint64
}

// Reset clears all fields of the PrefixRow for reuse.
// This is used with sync.Pool to avoid allocations.
func (r *PrefixRow) Reset() {
	r.Prefix = ""
	r.Depth = 0
	r.Count = 0
	r.TotalBytes = 0
	for i := range r.TierCounts {
		r.TierCounts[i] = 0
	}
	for i := range r.TierBytes {
		r.TierBytes[i] = 0
	}
}

// Merge adds the statistics from another PrefixRow into this one.
// The Prefix and Depth fields are not modified; only counts and bytes are summed.
// This is used during the k-way merge phase to combine duplicate prefixes.
func (r *PrefixRow) Merge(other *PrefixRow) {
	r.Count += other.Count
	r.TotalBytes += other.TotalBytes
	for i := range r.TierCounts {
		r.TierCounts[i] += other.TierCounts[i]
	}
	for i := range r.TierBytes {
		r.TierBytes[i] += other.TierBytes[i]
	}
}

// Clone creates a deep copy of the PrefixRow.
func (r *PrefixRow) Clone() *PrefixRow {
	clone := &PrefixRow{
		Prefix:     r.Prefix,
		Depth:      r.Depth,
		Count:      r.Count,
		TotalBytes: r.TotalBytes,
	}
	copy(clone.TierCounts[:], r.TierCounts[:])
	copy(clone.TierBytes[:], r.TierBytes[:])
	return clone
}

// SortPrefixRows sorts a slice of PrefixRows by prefix in lexicographic order.
// This is used to prepare rows for the streaming index builder.
func SortPrefixRows(rows []*PrefixRow) {
	slices.SortFunc(rows, func(a, b *PrefixRow) int {
		return strings.Compare(a.Prefix, b.Prefix)
	})
}

// PrefixStats holds aggregated statistics for a single prefix during the
// in-memory aggregation phase. This is a lightweight version of PrefixRow
// that can be used with map[string]*PrefixStats.
type PrefixStats struct {
	// Depth is the directory depth.
	Depth uint16

	// Count is the total number of objects under this prefix.
	Count uint64

	// TotalBytes is the total size in bytes.
	TotalBytes uint64

	// TierCounts holds object counts per storage tier.
	TierCounts [MaxTiers]uint64

	// TierBytes holds byte counts per storage tier.
	TierBytes [MaxTiers]uint64
}

// Reset clears all fields of the PrefixStats for reuse.
func (s *PrefixStats) Reset() {
	s.Depth = 0
	s.Count = 0
	s.TotalBytes = 0
	for i := range s.TierCounts {
		s.TierCounts[i] = 0
	}
	for i := range s.TierBytes {
		s.TierBytes[i] = 0
	}
}

// Add accumulates statistics from a single object.
func (s *PrefixStats) Add(size uint64, tierID tiers.ID) {
	s.Count++
	s.TotalBytes += size
	s.TierCounts[tierID]++
	s.TierBytes[tierID] += size
}

// ToPrefixRow creates a PrefixRow from this PrefixStats with the given prefix.
func (s *PrefixStats) ToPrefixRow(prefix string) *PrefixRow {
	row := &PrefixRow{
		Prefix:     prefix,
		Depth:      s.Depth,
		Count:      s.Count,
		TotalBytes: s.TotalBytes,
	}
	copy(row.TierCounts[:], s.TierCounts[:])
	copy(row.TierBytes[:], s.TierBytes[:])
	return row
}

// Config holds configuration for the external sort pipeline.
type Config struct {
	// TempDir is the directory for temporary run files.
	// If empty, os.TempDir() is used.
	TempDir string

	// MemoryThreshold is the approximate memory limit (in bytes) for the
	// in-memory prefix aggregator before flushing to a run file.
	// Default: 256MB.
	MemoryThreshold int64

	// RunFileBufferSize is the buffer size for reading/writing run files.
	// Larger buffers improve sequential I/O throughput.
	// Default: 4MB.
	RunFileBufferSize int

	// S3DownloadConcurrency is the number of concurrent S3 chunk downloads.
	// Default: 4.
	S3DownloadConcurrency int

	// ParseConcurrency is the number of concurrent CSV parsing goroutines.
	// Default: 4.
	ParseConcurrency int

	// IndexWriteConcurrency is the number of concurrent index file writers.
	// Default: 4.
	IndexWriteConcurrency int

	// MaxDepth is the maximum prefix depth to track.
	// Prefixes deeper than this are not aggregated.
	// Default: 0 (unlimited).
	MaxDepth int
}

// DefaultConfig returns a Config with sensible defaults based on the current machine.
// Concurrency is scaled with CPU count, and memory threshold is scaled with total system RAM.
func DefaultConfig() Config {
	numCPU := runtime.NumCPU()

	// Scale concurrency with CPU count, with reasonable bounds
	concurrency := numCPU
	if concurrency < 2 {
		concurrency = 2
	} else if concurrency > 16 {
		concurrency = 16
	}

	// Memory threshold: use ~25% of total RAM, clamped to [128MB, 1GB]
	// This is conservative to avoid OOM on systems with limited memory.
	memResult := sysmem.Total()
	memThreshold := int64(memResult.TotalBytes) / 4 // 25% of total memory

	// Clamp to reasonable bounds
	const minThreshold = 128 * 1024 * 1024  // 128MB minimum
	const maxThreshold = 1024 * 1024 * 1024 // 1GB maximum

	if memThreshold < minThreshold {
		memThreshold = minThreshold
	} else if memThreshold > maxThreshold {
		memThreshold = maxThreshold
	}

	return Config{
		TempDir:               "",
		MemoryThreshold:       memThreshold,
		RunFileBufferSize:     4 * 1024 * 1024, // 4MB
		S3DownloadConcurrency: concurrency,
		ParseConcurrency:      concurrency,
		IndexWriteConcurrency: concurrency,
		MaxDepth:              0, // unlimited
	}
}
