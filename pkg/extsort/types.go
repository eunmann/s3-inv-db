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

// MaxTiers mirrors tiers.NumTiers as an int so PrefixRow can declare
// fixed-size arrays for tier data (avoids map allocs).
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

// Reset clears the row for reuse via sync.Pool. Struct-zero assignment
// is one statement and zeroes the embedded arrays in the same op.
func (r *PrefixRow) Reset() {
	*r = PrefixRow{}
}

// Merge accumulates counts and bytes from other into r; Prefix and
// Depth are left untouched.
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

// recordLenBytes is the uint32 prefix-length header of a run record;
// recordScratchMin is the smallest scratch buffer worth allocating.
const (
	recordLenBytes   = 4
	recordScratchMin = 512
)

// readPrefixRowRecordInto reads one run record into row, keeping the
// merge loop free of row allocations. Buf is scratch space, grown as
// needed. Returns io.EOF when the source is exhausted.
func readPrefixRowRecordInto(reader io.Reader, buf *[]byte, row *PrefixRow) error {
	// The length is read into the reusable scratch buffer: a local array
	// passed to an io.Reader escapes, one allocation per row.
	if len(*buf) < recordLenBytes {
		*buf = make([]byte, recordScratchMin)
	}
	lenBuf := (*buf)[:recordLenBytes]
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return fmt.Errorf("read prefix length: %w", err)
	}
	prefixLen := int(binary.LittleEndian.Uint32(lenBuf))

	fixedSize := 2 + 8 + 8 + MaxTiers*8 + MaxTiers*8
	recordSize := prefixLen + fixedSize

	if len(*buf) < recordSize {
		*buf = make([]byte, recordSize*2)
	}

	if _, err := io.ReadFull(reader, (*buf)[:recordSize]); err != nil {
		return fmt.Errorf("read record: %w", err)
	}

	row.Reset()
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

	return nil
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

// add accumulates one object into the row.
func (r *PrefixRow) add(size uint64, tierID tiers.ID) {
	r.Count++
	r.TotalBytes += size
	r.TierCounts[tierID]++
	r.TierBytes[tierID] += size
}

// Config holds pipeline configuration. Grouped into substructs by
// concern: Merge concurrency, Observe (progress+events).
type Config struct {
	Observe  ObserveConfig
	TempDir  string
	Merge    MergeConfig
	MaxDepth int
	// AllowAnomalies builds even when inventory rows can't be read
	// exactly (s3inventory.Anomalies): they are skipped or indexed with
	// size 0 and logged. Off by default: such a build would report
	// numbers that differ from a direct scan of the inventory.
	AllowAnomalies bool
}

// ErrInventoryAnomalies is returned when an inventory chunk has rows
// that can't be read exactly and Config.AllowAnomalies is off.
var ErrInventoryAnomalies = errors.New("inventory rows can't be read exactly")

// ErrChunkSizeMismatch is returned when a downloaded inventory chunk's
// size differs from the size its manifest lists: a truncated or
// replaced file would silently drop or change rows.
var ErrChunkSizeMismatch = errors.New("inventory chunk size differs from its manifest")

// MergeConfig tunes the K-way merge phase.
type MergeConfig struct {
	// NumWorkers: concurrent merge workers. Default max(NumCPU/2, 1).
	NumWorkers int

	// MaxFanIn: runs merged per worker. Higher = fewer rounds, more
	// memory per worker. Default 16.
	MaxFanIn int
}

// withDefaults replaces unset (or, for MaxFanIn, unusable) fields with
// DefaultConfig's.
func (c MergeConfig) withDefaults() MergeConfig {
	def := DefaultConfig().Merge
	if c.NumWorkers <= 0 {
		c.NumWorkers = def.NumWorkers
	}
	if c.MaxFanIn <= 1 {
		c.MaxFanIn = def.MaxFanIn
	}

	return c
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
	mergeWorkers := max(runtime.NumCPU()/2, 1)

	return Config{
		TempDir:  "",
		MaxDepth: 0,
		Merge: MergeConfig{
			NumWorkers: mergeWorkers,
			MaxFanIn:   16,
		},
	}
}
