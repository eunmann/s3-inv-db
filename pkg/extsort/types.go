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
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// MaxTiers mirrors tiers.NumTiers as an int so PrefixRow / PrefixStats
// can declare fixed-size arrays for tier data (avoids map allocs).
const MaxTiers = int(tiers.NumTiers)

// RowIterator yields PrefixRows in sorted order; Next returns io.EOF
// once exhausted. Remaining returns an upper-bound row count (0 if
// unknown) so the IndexBuilder can pre-size its arrays.
type RowIterator interface {
	Next() (*PrefixRow, error)
	Remaining() uint64
}

// PrefixRow is the primary unit passed through the external sort.
// Field order is tuned for alignment (Depth at the end avoids 6 bytes
// of inter-field padding at billion-row scale). Tier index i ↔ tiers.ID(i).
type PrefixRow struct {
	Prefix     string
	Count      uint64
	TotalBytes uint64
	TierCounts [MaxTiers]uint64
	TierBytes  [MaxTiers]uint64
	Depth      uint16
}

// Reset clears the row for reuse via sync.Pool.
func (r *PrefixRow) Reset() {
	r.Prefix = ""
	r.Depth = 0
	r.Count = 0
	r.TotalBytes = 0
	clear(r.TierCounts[:])
	clear(r.TierBytes[:])
}

// Merge sums counts and bytes from other into r. Prefix and Depth
// are not modified — only the per-prefix stats accumulate.
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
		if errors.Is(err, io.EOF) {
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

// PrefixStats holds aggregated stats for one prefix during the
// in-memory aggregation phase. Uint64 counters because a single tier
// of the root prefix can exceed 2^32 at PB scale.
type PrefixStats struct {
	Count      uint64
	TotalBytes uint64
	TierCounts [MaxTiers]uint64
	TierBytes  [MaxTiers]uint64
	Depth      uint16
}

// Reset clears all fields of the PrefixStats for reuse.
func (s *PrefixStats) Reset() {
	s.Depth = 0
	s.Count = 0
	s.TotalBytes = 0
	clear(s.TierCounts[:])
	clear(s.TierBytes[:])
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

// Config holds pipeline configuration. Grouped into substructs by
// concern: S3 download, Merge concurrency, Observe (progress+events).
type Config struct {
	Observe ObserveConfig
	TempDir string
	Merge   MergeConfig
	S3      S3Config
	// PrefixDictionary toggles dictionary-encoded prefix storage
	// (smaller blob, slower prefix-string reads). Default true.
	PrefixDictionary bool
	MaxDepth         int
}

// S3Config tunes the S3 download manager.
type S3Config struct {
	// DownloadPartConcurrency: parallel range downloads per object.
	// Default max(2, NumCPU/4).
	DownloadPartConcurrency int

	// DownloadPartSize per part. Default 16 MiB.
	DownloadPartSize int64
}

// MergeConfig tunes the K-way merge phase.
type MergeConfig struct {
	// NumWorkers: concurrent merge workers. Default max(NumCPU/2, 1).
	NumWorkers int

	// MaxFanIn: runs merged per worker. Higher = fewer rounds, more
	// memory per worker. Default 16.
	MaxFanIn int

	// UseCompressedRuns enables zstd on intermediate run files. Default true.
	UseCompressedRuns bool
}

// ObserveConfig wires optional progress + event consumers.
type ObserveConfig struct {
	// OnProgress fires on phase transitions and once per ingest chunk.
	// done/total are zero on transitions.
	OnProgress func(stage string, done, total int64)

	// EventBus, when non-nil, receives structured events. Caller owns
	// its lifecycle; the pipeline does not Close it.
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
	mergeWorkers := max(numCPU/2, 1)

	return Config{
		TempDir:          "",
		MaxDepth:         0,
		PrefixDictionary: true,
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
