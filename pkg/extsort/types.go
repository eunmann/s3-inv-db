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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"unsafe"

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
	shared := sharedPrefixLen(prev, row.Prefix)

	return encodeRunRecordShared(dst, shared, row.Prefix[shared:], row)
}

// encodeRunRecordShared appends the record of row whose prefix is the
// previous record's first shared bytes followed by suffix; row.Prefix
// is not read.
func encodeRunRecordShared(dst []byte, shared int, suffix string, row *PrefixRow) []byte {
	dst = binary.AppendUvarint(dst, uint64(shared))
	dst = binary.AppendUvarint(dst, uint64(len(suffix)))
	dst = append(dst, suffix...)

	return appendRunRecordStats(dst, row)
}

// appendRunRecordStats appends the part of row's record after its
// prefix: the depth, the tier mask and the counts.
func appendRunRecordStats(dst []byte, row *PrefixRow) []byte {
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

// sharedPrefixLen returns the length of a and b's common prefix. A run's
// prefixes share most of their path with the previous one, so it
// compares eight bytes at a time: byte by byte, this was 7% of a deep
// build's CPU.
func sharedPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for ; i+8 <= n; i += 8 {
		if x := load64(a, i) ^ load64(b, i); x != 0 {
			return i + bits.TrailingZeros64(x)/8
		}
	}
	for i < n && a[i] == b[i] {
		i++
	}

	return i
}

// load64 reads s[i:i+8] as a little-endian word, without copying.
func load64(s string, i int) uint64 {
	return binary.LittleEndian.Uint64(unsafe.Slice(unsafe.StringData(s[i:]), 8))
}

// runRecordWindow is the most of a run's stream a record is decoded
// from in place: more than any record of a key S3 allows (1024 bytes)
// needs, and small enough that refilling the read buffer to hold it
// copies little.
const runRecordWindow = 4 << 10

// runRecordDecoder reads a run's records from a buffered stream. It
// decodes each from the read buffer in place (Peek), not byte by byte
// through the reader, and carves the prefixes from an arena: reading
// varints a byte per call and allocating each prefix were most of a
// sorted build's serial final merge.
type runRecordDecoder struct {
	in    *bufio.Reader
	prev  string // the last prefix read
	arena prefixArena
}

// next reads the next record into row: io.EOF when the stream ends
// before a record, errRunRecordCorrupt when it ends inside one.
func (d *runRecordDecoder) next(row *PrefixRow) error {
	window, err := d.in.Peek(min(runRecordWindow, d.in.Size()))
	if len(window) == 0 {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return fmt.Errorf("read run record: %w", err)
	}
	n, decodeErr := decodeRunRecord(window, d.prev, &d.arena, row)
	if errors.Is(decodeErr, errRunRecordShort) && err == nil {
		// Longer than the window: decode it from the whole buffer.
		window, err = d.in.Peek(d.in.Size())
		n, decodeErr = decodeRunRecord(window, d.prev, &d.arena, row)
	}
	if errors.Is(decodeErr, errRunRecordShort) {
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read run record: %w", err)
		}

		return fmt.Errorf("%w: stream ends inside a record", errRunRecordCorrupt)
	}
	if decodeErr != nil {
		return decodeErr
	}
	if _, err := d.in.Discard(n); err != nil {
		return fmt.Errorf("read run record: %w", err)
	}
	d.prev = row.Prefix

	return nil
}

// errRunRecordShort means the bytes end inside the record.
var errRunRecordShort = errors.New("run record continues past the bytes given")

// decodeRunRecord decodes the record at the start of b, front-coded
// against prev, into row and returns its length: errRunRecordShort when
// b ends inside it. The prefix comes from arena.
func decodeRunRecord(b []byte, prev string, arena *prefixArena, row *PrefixRow) (int, error) {
	c := recordCursor{b: b}
	shared, suffixLen := c.uvarint(), c.uvarint()
	if err := c.check("prefix lengths"); err != nil {
		return 0, err
	}
	if shared > uint64(len(prev)) || suffixLen > math.MaxInt32 {
		return 0, fmt.Errorf("%w: %d shared of %d bytes, suffix %d", errRunRecordCorrupt, shared, len(prev), suffixLen)
	}
	if suffixLen > uint64(len(b)-c.off) {
		return 0, errRunRecordShort
	}
	row.Reset()
	if n := int(shared + suffixLen); n > 0 {
		p := arena.alloc(n)
		copy(p, prev[:shared])
		copy(p[shared:], b[c.off:c.off+int(suffixLen)])
		row.Prefix = unsafe.String(unsafe.SliceData(p), n)
	}
	c.off += int(suffixLen)
	depth, mask := c.uvarint(), c.uvarint()
	if err := c.check("depth and tier mask"); err != nil {
		return 0, err
	}
	if depth > math.MaxUint16 {
		return 0, fmt.Errorf("%w: depth %d", errRunRecordCorrupt, depth)
	}
	row.Depth = uint16(depth)
	if mask&^(1<<MaxTiers-1|runRecordTotals) != 0 {
		return 0, fmt.Errorf("%w: tier mask %#x", errRunRecordCorrupt, mask)
	}
	for m := mask & (1<<MaxTiers - 1); m != 0; m &= m - 1 {
		t := bits.TrailingZeros64(m)
		row.TierCounts[t], row.TierBytes[t] = c.uvarint(), c.uvarint()
		row.Count += row.TierCounts[t]
		row.TotalBytes += row.TierBytes[t]
	}
	if mask&runRecordTotals != 0 {
		row.Count, row.TotalBytes = c.uvarint(), c.uvarint()
	}
	if err := c.check("counts"); err != nil {
		return 0, err
	}

	return c.off, nil
}

// recordCursor reads a record's varints from b at off. A read past the
// end marks it short, an overlong varint bad; either way the reads
// after it return 0, and check reports it.
type recordCursor struct {
	b          []byte
	off        int
	short, bad bool
}

func (c *recordCursor) uvarint() uint64 {
	// Most fields (lengths, depth, mask, small counts) fit in one byte.
	if c.off < len(c.b) && c.b[c.off] < 0x80 {
		c.off++

		return uint64(c.b[c.off-1])
	}
	v, k := binary.Uvarint(c.b[c.off:])
	if k <= 0 {
		c.short = c.short || k == 0
		c.bad = c.bad || k < 0
		c.off = len(c.b)

		return 0
	}
	c.off += k

	return v
}

// check returns the error the reads of what so far met, if any.
func (c *recordCursor) check(what string) error {
	switch {
	case c.bad:
		return fmt.Errorf("%w: %s: varint overflows", errRunRecordCorrupt, what)
	case c.short:
		return errRunRecordShort
	}

	return nil
}

// prefixArena carves prefix strings from shared chunks, one allocation
// per chunk instead of one per row. Bytes handed out are never written
// again, and a chunk lives while any string in it does: the merge and
// the index builder hold a few rows at a time, so a few chunks.
type prefixArena struct {
	free []byte
}

// prefixArenaChunk is an arena chunk's size; a prefix longer than an
// eighth of it gets its own allocation, so a chunk wastes little.
const prefixArenaChunk = 16 << 10

func (a *prefixArena) alloc(n int) []byte {
	if n > len(a.free) {
		if n > prefixArenaChunk/8 {
			return make([]byte, n)
		}
		a.free = make([]byte, prefixArenaChunk)
	}
	p := a.free[:n:n]
	a.free = a.free[n:]

	return p
}

// SortPrefixRows sorts a slice of PrefixRows by prefix in lexicographic
// order, through (prefix, row) pairs: comparing through the row pointers
// would load a row from scattered memory for every comparison.
func SortPrefixRows(rows []*PrefixRow) {
	keyed := make([]keyedRow, len(rows))
	for i, r := range rows {
		keyed[i] = keyedRow{prefix: r.Prefix, row: r}
	}
	slices.SortFunc(keyed, func(a, b keyedRow) int {
		return strings.Compare(a.prefix, b.prefix)
	})
	for i := range keyed {
		rows[i] = keyed[i].row
	}
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
	// OnProgress fires on phase transitions, once per ingest chunk, and
	// as the finalize steps advance. Done/Total are zero on transitions.
	OnProgress events.ProgressFunc

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
