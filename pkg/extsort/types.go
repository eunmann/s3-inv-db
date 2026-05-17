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

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
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
// Field order is tuned for alignment: large fields first, then the
// fixed-size tier arrays, then the small uint16 Depth at the end.
// Putting Depth between Prefix and Count would waste 6 bytes of
// padding inside each row (real bytes at billion-row scale).
//
// The struct uses fixed-size arrays for tier data to avoid map
// allocations in hot paths. Tier index i corresponds to tiers.ID(i).
type PrefixRow struct {
	// Prefix is the full prefix string (e.g., "data/2024/01/").
	Prefix string

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

	// Depth is the directory depth (number of '/' characters).
	// uint16 supports up to 65535 levels — far beyond any realistic key.
	Depth uint16
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
	return readPrefixRowRecordInto(reader, buf, nil)
}

// readPrefixRowRecordInto is like readPrefixRowRecord but reuses the given
// PrefixRow when non-nil. Pass a row obtained from prefixRowPool to keep
// the merge loop allocation-free.
func readPrefixRowRecordInto(reader io.Reader, buf *[]byte, row *PrefixRow) (*PrefixRow, error) {
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

	if row == nil {
		row = &PrefixRow{}
	} else {
		row.Reset()
	}
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

// encodePrefixRowRecord serialises one PrefixRow into buf using the
// shared on-the-wire layout (length-prefixed string + fixed fields +
// tier arrays). The caller passes &buf so the slice can be grown
// in place across many calls; the encoded byte length is returned so
// callers can track uncompressed size or pass a partial slice to
// the underlying writer.
//
// Both RunFileWriter and CompressedRunWriter delegate to this — the
// binary format is identical and only the I/O layer differs.
func encodePrefixRowRecord(buf *[]byte, row *PrefixRow) int {
	prefixLen := len(row.Prefix)
	recordSize := 4 + prefixLen + 2 + 8 + 8 + MaxTiers*8 + MaxTiers*8
	if len(*buf) < recordSize {
		*buf = make([]byte, recordSize*2)
	}
	b := *buf
	offset := 0

	binary.LittleEndian.PutUint32(b[offset:], uint32(prefixLen))
	offset += 4
	copy(b[offset:], row.Prefix)
	offset += prefixLen

	binary.LittleEndian.PutUint16(b[offset:], row.Depth)
	offset += 2

	binary.LittleEndian.PutUint64(b[offset:], row.Count)
	offset += 8

	binary.LittleEndian.PutUint64(b[offset:], row.TotalBytes)
	offset += 8

	for i := range MaxTiers {
		binary.LittleEndian.PutUint64(b[offset:], row.TierCounts[i])
		offset += 8
	}
	for i := range MaxTiers {
		binary.LittleEndian.PutUint64(b[offset:], row.TierBytes[i])
		offset += 8
	}

	return offset
}

// PrefixStats holds aggregated statistics for a single prefix during
// the in-memory aggregation phase. All counters are uint64 because at
// billion-object / PB-scale buckets, a single tier of the root prefix
// can exceed 2^32. Field order matches PrefixRow for alignment
// efficiency (Depth at the end avoids inter-field padding).
type PrefixStats struct {
	// Count is the total number of objects under this prefix.
	Count uint64

	// TotalBytes is the total size in bytes.
	TotalBytes uint64

	// TierCounts holds object counts per storage tier.
	TierCounts [MaxTiers]uint64

	// TierBytes holds byte counts per storage tier.
	TierBytes [MaxTiers]uint64

	// Depth is the directory depth.
	Depth uint16
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
// Concerns are grouped into substructs: S3 download tuning, Merge
// concurrency, Observe (progress + events). Top-level fields are
// the ones that apply across the whole pipeline.
type Config struct {
	// TempDir is the directory for temporary run files. If empty,
	// os.TempDir() is used.
	TempDir string

	// MaxDepth is the maximum prefix depth to track. Prefixes
	// deeper than this are not aggregated. 0 means unlimited.
	MaxDepth int

	S3      S3Config
	Merge   MergeConfig
	Observe ObserveConfig
}

// S3Config tunes the S3 download manager used during ingest.
type S3Config struct {
	// DownloadPartConcurrency is the number of concurrent parts for
	// each S3 download (parallel range downloads within one object).
	// Default: max(2, NumCPU/4).
	DownloadPartConcurrency int

	// DownloadPartSize is the size of each part for parallel S3
	// downloads. Larger values may improve throughput but use more
	// memory. Default: 16 MiB.
	DownloadPartSize int64
}

// MergeConfig tunes the K-way merge phase.
type MergeConfig struct {
	// NumWorkers is the number of concurrent merge workers.
	// Default: min(max(NumCPU/2, 1), 8).
	NumWorkers int

	// MaxFanIn is the maximum number of runs merged in a single
	// worker. Higher values reduce merge rounds but increase memory
	// per worker. Default: 16.
	MaxFanIn int

	// UseCompressedRuns enables zstd compression for intermediate
	// run files. Default: true.
	UseCompressedRuns bool
}

// ObserveConfig wires up optional progress + event consumers.
type ObserveConfig struct {
	// OnProgress is invoked on every phase transition and roughly
	// once per ingest chunk. done/total are zero on stage transitions;
	// otherwise they describe quantitative progress.
	OnProgress func(stage string, done, total int64)

	// EventBus, if non-nil, receives structured events from every
	// pipeline stage. The pipeline does not close the bus; the
	// caller owns its lifecycle.
	EventBus *events.Bus
}

// DefaultConfig returns a Config with sensible defaults based on the
// current machine. Concurrency is derived from runtime.NumCPU per the
// pipeline's "throughput scales with instance size" contract; the
// process memory limit is governed by GOMEMLIMIT (see
// sysmem.ApplyMemoryLimit) rather than a fractional partition here.
func DefaultConfig() Config {
	numCPU := runtime.NumCPU()
	const minPartConcurrency = 2
	partConcurrency := max(numCPU/4, minPartConcurrency)
	// Merge workers scale with CPU but cap to avoid pathological
	// parallel-merge fan-in on very large machines.
	const mergeWorkerCap = 8
	mergeWorkers := min(max(numCPU/2, 1), mergeWorkerCap)

	return Config{
		TempDir:  "",
		MaxDepth: 0,
		S3: S3Config{
			DownloadPartConcurrency: partConcurrency,
			DownloadPartSize:        16 * 1024 * 1024,
		},
		Merge: MergeConfig{
			NumWorkers:        mergeWorkers,
			MaxFanIn:          16,
			UseCompressedRuns: true,
		},
	}
}
