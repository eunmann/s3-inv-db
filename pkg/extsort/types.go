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
	"math"
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

// A run record is the row in varints, its prefix front-coded against the
// previous record of the same run:
//
//	shared    uvarint  bytes of the previous prefix this one starts with
//	suffixLen uvarint
//	suffix    suffixLen bytes
//	depth     uvarint
//	tierMask  uvarint  bit t set for each tier with a count or bytes;
//	                   runRecordTotals set if the totals follow
//	per set tier, ascending: count uvarint, bytes uvarint
//	[count uvarint, bytes uvarint]  only with runRecordTotals
//
// Every object adds to its row's totals and to exactly one tier, so
// Count and TotalBytes are the sums of the tier counts and bytes and are
// not stored; a row where they differ stores them, so any row reads
// back exactly.

// runRecordTotals is the tier-mask flag of a record that stores its
// totals.
const runRecordTotals = 1 << 15

// errRunRecordCorrupt means a run record cannot be decoded.
var errRunRecordCorrupt = errors.New("corrupt run record")

// encodeRunRecord appends row's record, front-coded against prev, to dst.
func encodeRunRecord(dst []byte, prev string, row *PrefixRow) []byte {
	var count, bytes uint64
	var mask uint64
	for t := range MaxTiers {
		count += row.TierCounts[t]
		bytes += row.TierBytes[t]
		if row.TierCounts[t]|row.TierBytes[t] != 0 {
			mask |= 1 << t
		}
	}
	explicit := count != row.Count || bytes != row.TotalBytes
	if explicit {
		mask |= runRecordTotals
	}
	shared := 0
	for n := min(len(prev), len(row.Prefix)); shared < n && prev[shared] == row.Prefix[shared]; {
		shared++
	}
	dst = binary.AppendUvarint(dst, uint64(shared))
	dst = binary.AppendUvarint(dst, uint64(len(row.Prefix)-shared))
	dst = append(dst, row.Prefix[shared:]...)
	dst = binary.AppendUvarint(dst, uint64(row.Depth))
	dst = binary.AppendUvarint(dst, mask)
	for t := range MaxTiers {
		if mask&(1<<t) != 0 {
			dst = binary.AppendUvarint(dst, row.TierCounts[t])
			dst = binary.AppendUvarint(dst, row.TierBytes[t])
		}
	}
	if explicit {
		dst = binary.AppendUvarint(dst, row.Count)
		dst = binary.AppendUvarint(dst, row.TotalBytes)
	}

	return dst
}

// runRecordSource is what records are read from: a buffered stream.
type runRecordSource interface {
	io.Reader
	io.ByteReader
}

// decodeRunRecord reads the next record, front-coded against prev, into
// row, keeping the merge loop free of row allocations besides the
// prefix string. Buf is scratch space for the suffix, grown as needed.
// Returns io.EOF when the source ends before a record.
func decodeRunRecord(r runRecordSource, prev string, buf *[]byte, row *PrefixRow) error {
	shared, err := binary.ReadUvarint(r)
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if err != nil {
		return fmt.Errorf("read run record: %w", err)
	}
	suffixLen, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("%w: suffix length: %w", errRunRecordCorrupt, err)
	}
	if shared > uint64(len(prev)) || suffixLen > math.MaxInt32 {
		return fmt.Errorf("%w: %d shared of %d bytes, suffix %d", errRunRecordCorrupt, shared, len(prev), suffixLen)
	}
	if uint64(cap(*buf)) < suffixLen {
		*buf = make([]byte, suffixLen)
	}
	suffix := (*buf)[:suffixLen]
	if _, err := io.ReadFull(r, suffix); err != nil {
		return fmt.Errorf("%w: suffix: %w", errRunRecordCorrupt, err)
	}
	row.Reset()
	var prefix strings.Builder
	prefix.Grow(int(shared) + len(suffix))
	prefix.WriteString(prev[:shared])
	prefix.Write(suffix)
	row.Prefix = prefix.String()
	depth, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("%w: depth: %w", errRunRecordCorrupt, err)
	}
	if depth > math.MaxUint16 {
		return fmt.Errorf("%w: depth %d", errRunRecordCorrupt, depth)
	}
	row.Depth = uint16(depth)
	mask, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("%w: tier mask: %w", errRunRecordCorrupt, err)
	}
	if mask&^(1<<MaxTiers-1|runRecordTotals) != 0 {
		return fmt.Errorf("%w: tier mask %#x", errRunRecordCorrupt, mask)
	}
	for t := range MaxTiers {
		if mask&(1<<t) == 0 {
			continue
		}
		if row.TierCounts[t], err = binary.ReadUvarint(r); err != nil {
			return fmt.Errorf("%w: tier %d count: %w", errRunRecordCorrupt, t, err)
		}
		if row.TierBytes[t], err = binary.ReadUvarint(r); err != nil {
			return fmt.Errorf("%w: tier %d bytes: %w", errRunRecordCorrupt, t, err)
		}
		row.Count += row.TierCounts[t]
		row.TotalBytes += row.TierBytes[t]
	}
	if mask&runRecordTotals != 0 {
		if row.Count, err = binary.ReadUvarint(r); err != nil {
			return fmt.Errorf("%w: count: %w", errRunRecordCorrupt, err)
		}
		if row.TotalBytes, err = binary.ReadUvarint(r); err != nil {
			return fmt.Errorf("%w: bytes: %w", errRunRecordCorrupt, err)
		}
	}

	return nil
}

// SortPrefixRows sorts a slice of PrefixRows by prefix in lexicographic order.
// This is used to prepare rows for the streaming index builder.
func SortPrefixRows(rows []*PrefixRow) {
	sortPrefixRows(rows, nil)
}

// sortPrefixRows sorts rows by prefix through (prefix, row) pairs in
// scratch, grown as needed and returned for reuse: comparing through
// the row pointers would load a row from scattered slab memory for
// every comparison. The returned scratch holds no references.
func sortPrefixRows(rows []*PrefixRow, scratch []keyedRow) []keyedRow {
	if cap(scratch) < len(rows) {
		scratch = make([]keyedRow, len(rows))
	}
	keyed := scratch[:len(rows)]
	for i, r := range rows {
		keyed[i] = keyedRow{prefix: r.Prefix, row: r}
	}
	slices.SortFunc(keyed, func(a, b keyedRow) int {
		return strings.Compare(a.prefix, b.prefix)
	})
	for i := range keyed {
		rows[i] = keyed[i].row
	}
	clear(keyed)

	return scratch[:0]
}

// keyedRow is a row with its prefix beside the pointer, for sorting.
type keyedRow struct {
	prefix string
	row    *PrefixRow
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
	// Under GOMEMLIMIT the merger lowers it so every disk round's open
	// readers fit the same budget as the final merge's.
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
