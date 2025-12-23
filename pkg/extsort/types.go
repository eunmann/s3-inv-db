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
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/membudget"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// MaxTiers is the maximum number of storage tiers supported.
// This matches tiers.NumTiers and is used for fixed-size arrays to avoid map allocations.
const MaxTiers = int(tiers.NumTiers)

// RowIterator provides sequential access to sorted PrefixRows.
// This interface is implemented by both MergeIterator and singleRunIterator.
type RowIterator interface {
	// Next returns the next PrefixRow in sorted order.
	// Returns io.EOF when all rows have been consumed.
	Next() (*PrefixRow, error)
}

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

// readPrefixRowRecord reads a single PrefixRow from a reader using the common binary format.
// The buf pointer is used for temporary storage and may be resized if needed.
// Returns io.EOF when the source is exhausted.
func readPrefixRowRecord(reader io.Reader, buf *[]byte) (*PrefixRow, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read prefix length: %w", err)
	}
	prefixLen := int(binary.LittleEndian.Uint32(lenBuf[:]))

	fixedSize := 2 + 8 + 8 + MaxTiers*8 + MaxTiers*8
	recordSize := prefixLen + fixedSize

	if len(*buf) < recordSize {
		*buf = make([]byte, recordSize*2)
	}

	if _, err := io.ReadFull(reader, (*buf)[:recordSize]); err != nil {
		return nil, fmt.Errorf("read record: %w", err)
	}

	row := &PrefixRow{}
	offset := 0

	row.Prefix = string((*buf)[offset : offset+prefixLen])
	offset += prefixLen

	row.Depth = binary.LittleEndian.Uint16((*buf)[offset:])
	offset += 2

	row.Count = binary.LittleEndian.Uint64((*buf)[offset:])
	offset += 8

	row.TotalBytes = binary.LittleEndian.Uint64((*buf)[offset:])
	offset += 8

	for i := range MaxTiers {
		row.TierCounts[i] = binary.LittleEndian.Uint64((*buf)[offset:])
		offset += 8
	}

	for i := range MaxTiers {
		row.TierBytes[i] = binary.LittleEndian.Uint64((*buf)[offset:])
		offset += 8
	}

	return row, nil
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

	// MemoryBudget is the central memory budget manager for the pipeline.
	// If nil, a default budget based on system RAM is created.
	MemoryBudget *membudget.Budget

	// MemoryThreshold is the approximate memory limit (in bytes) for the
	// in-memory prefix aggregator before flushing to a run file.
	//
	// Deprecated: Use MemoryBudget instead. This field is kept for backward
	// compatibility but is ignored when MemoryBudget is set.
	MemoryThreshold int64

	// RunFileBufferSize is the buffer size for reading/writing run files.
	// Default: 4MB.
	//
	// Deprecated: Buffer sizes are now calculated from MemoryBudget.
	// This field is kept for backward compatibility.
	RunFileBufferSize int

	// S3DownloadConcurrency is the number of concurrent S3 chunk downloads.
	// Default: 4.
	S3DownloadConcurrency int

	// S3DownloadPartConcurrency is the number of concurrent parts for each S3 download.
	// Used by the S3 Download Manager for parallel range downloads within a single object.
	// Default: max(4, NumCPU).
	S3DownloadPartConcurrency int

	// S3DownloadPartSize is the size of each part for parallel S3 downloads.
	// Larger values may improve throughput but use more memory.
	// Default: 16MB.
	S3DownloadPartSize int64

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

	// NumMergeWorkers is the number of concurrent merge workers.
	// Default: max(1, NumCPU/2).
	NumMergeWorkers int

	// MaxMergeFanIn is the maximum number of runs merged in a single worker.
	// Higher values reduce merge rounds but increase memory per worker.
	// Default: 8.
	MaxMergeFanIn int

	// UseCompressedRuns enables compression for intermediate run files.
	// This significantly reduces disk I/O and storage at minimal CPU cost.
	// Default: true.
	UseCompressedRuns bool
}

// DefaultConfig returns a Config with sensible defaults based on the current machine.
// Concurrency is scaled with CPU count, and memory budget is set to 50% of system RAM.
func DefaultConfig() Config {
	numCPU := runtime.NumCPU()

	// Scale concurrency with CPU count, with reasonable bounds
	concurrency := numCPU
	if concurrency < 2 {
		concurrency = 2
	} else if concurrency > 16 {
		concurrency = 16
	}

	// Part concurrency for parallel range downloads within a single object
	partConcurrency := numCPU
	if partConcurrency < 4 {
		partConcurrency = 4
	}
	if partConcurrency > 16 {
		partConcurrency = 16
	}

	// Merge workers scale with CPU but cap lower to avoid memory pressure
	mergeWorkers := numCPU / 2
	if mergeWorkers < 1 {
		mergeWorkers = 1
	}
	if mergeWorkers > 8 {
		mergeWorkers = 8
	}

	return Config{
		TempDir:                   "",
		MemoryBudget:              membudget.NewFromSystemRAM(),
		RunFileBufferSize:         4 * 1024 * 1024, // 4MB
		S3DownloadConcurrency:     concurrency,
		S3DownloadPartConcurrency: partConcurrency,
		S3DownloadPartSize:        16 * 1024 * 1024, // 16MB
		ParseConcurrency:          concurrency,
		IndexWriteConcurrency:     concurrency,
		MaxDepth:                  0, // unlimited
		NumMergeWorkers:           mergeWorkers,
		MaxMergeFanIn:             16, // Higher fan-in reduces merge rounds
		UseCompressedRuns:         true,
	}
}

// EnsureBudget ensures that Config has a valid MemoryBudget.
// If MemoryBudget is nil, it creates one from system RAM.
func (c *Config) EnsureBudget() {
	if c.MemoryBudget == nil {
		c.MemoryBudget = membudget.NewFromSystemRAM()
	}
}

// AggregatorMemoryThreshold returns the memory threshold for the aggregator.
// This is the budget's aggregator allocation (50% of total budget).
func (c *Config) AggregatorMemoryThreshold() int64 {
	c.EnsureBudget()
	return int64(c.MemoryBudget.AggregatorBudget())
}

// RunBufferBudget returns the total budget for run file buffers.
func (c *Config) RunBufferBudget() int64 {
	c.EnsureBudget()
	return int64(c.MemoryBudget.RunBufferBudget())
}

// MergeBudget returns the total budget for merge phase operations.
func (c *Config) MergeBudget() int64 {
	c.EnsureBudget()
	return int64(c.MemoryBudget.MergeBudget())
}

// IndexBuildBudget returns the total budget for index building.
func (c *Config) IndexBuildBudget() int64 {
	c.EnsureBudget()
	return int64(c.MemoryBudget.IndexBuildBudget())
}

// S3DownloaderConfig returns the S3 downloader configuration derived from this Config.
// Use this when creating an S3 client to ensure consistent configuration.
func (c *Config) S3DownloaderConfig() S3DownloaderConfig {
	cfg := S3DownloaderConfig{
		TempDir: c.TempDir,
	}

	if c.S3DownloadPartConcurrency > 0 {
		cfg.Concurrency = c.S3DownloadPartConcurrency
	} else {
		cfg.Concurrency = DefaultConfig().S3DownloadPartConcurrency
	}

	if c.S3DownloadPartSize > 0 {
		cfg.PartSize = c.S3DownloadPartSize
	} else {
		cfg.PartSize = DefaultConfig().S3DownloadPartSize
	}

	return cfg
}

// S3DownloaderConfig mirrors s3fetch.DownloaderConfig to avoid import cycles.
// This is used to configure the S3 client with the pipeline's settings.
type S3DownloaderConfig struct {
	Concurrency int
	PartSize    int64
	TempDir     string
}
