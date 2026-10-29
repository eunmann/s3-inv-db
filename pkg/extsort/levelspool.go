package extsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// levelSpool holds the build's prefixes by depth until Finalize. Rows
// arrive in key order (preorder) but the index stores them in level
// order: every depth-0 row, then every depth-1 row, each depth in key
// order. Each depth's records stream uncompressed to their own file
// through a levelSpoolBufSize bufio writer; Finalize reads the depths
// back in turn and writes the index files in level order, removing
// each depth's file once read. Memory is one buffer per depth,
// whatever the prefix count, plus the hand-off batches.
//
// Add fills records (next, commit) in batches that a writer goroutine
// encodes and writes, so the builder's serial loop does only the tree
// bookkeeping and the hashing: writing was a third of its time.
type levelSpool struct {
	err error // the writer's first error; read once it has stopped
	dir string
	// plan, when set, sees each entry on the writer goroutine before its
	// record is written, every entry once in Add order; its error fails
	// the spool as a write error does.
	plan func(e *spoolEntry) error
	// batch is the one Add is filling; full and free carry batches to
	// the writer and back.
	batch   []spoolEntry
	full    chan []spoolEntry
	free    chan []spoolEntry
	depths  []*spoolFile // index = depth
	rec     []byte       // the record add is encoding
	writer  sync.WaitGroup
	failed  atomic.Bool
	started bool
	stopped bool
}

// spoolBatchRows and spoolBatches size the hand-off to the writer:
// three batches (one being filled, one queued, one being written) of
// 1024 rows, about 1 MiB in all.
const (
	spoolBatchRows = 1024
	spoolBatches   = 3
)

// spoolEntry is a row on its way to the spool: its record, its depth
// and what the plan takes from it that is not spooled.
type spoolEntry struct {
	rec   spoolRecord
	depth int
	// keyHash, fp and local (the prefix's index within its depth) are
	// for the plan to hand the MPHF builder.
	keyHash, fp, local uint64
	// name is the last segment, without its slash (empty for the root),
	// for the plan to intern into rec.segID.
	name []byte
	// tierCounts and tierBytes are the row's tier stats by tier ID, for
	// the plan to turn into rec's tier slots.
	tierCounts, tierBytes [tiers.NumTiers]uint64
}

// errSpoolStopped is returned by commit once the spool was finished.
var errSpoolStopped = errors.New("level spool already finished")

func newLevelSpool(dir string) *levelSpool {
	return &levelSpool{dir: dir}
}

// next returns the entry for the next row; commit hands it over once
// filled. The writer starts with the first row.
func (s *levelSpool) next() *spoolEntry {
	if !s.started && !s.stopped {
		s.started = true
		s.full = make(chan []spoolEntry, spoolBatches-1)
		s.free = make(chan []spoolEntry, spoolBatches)
		for range spoolBatches - 1 {
			s.free <- make([]spoolEntry, 0, spoolBatchRows)
		}
		s.batch = make([]spoolEntry, 0, spoolBatchRows)
		s.writer.Go(s.write)
	}
	s.batch = s.batch[:len(s.batch)+1]

	return &s.batch[len(s.batch)-1]
}

// commit hands the entry next returned to the writer, sending the
// batch once full. It returns the writer's error once the writer has
// failed, a batch or so after the row it failed on.
func (s *levelSpool) commit() error {
	if s.stopped {
		s.batch = s.batch[:0]
		if s.err != nil {
			return s.err
		}

		return errSpoolStopped
	}
	if len(s.batch) < cap(s.batch) {
		return nil
	}
	if s.failed.Load() {
		return s.stop()
	}
	s.full <- s.batch
	s.batch = (<-s.free)[:0]

	return nil
}

// write is the writer goroutine: it writes each batch's records in
// order, and after an error only returns batches until the spool
// stops.
func (s *levelSpool) write() {
	for batch := range s.full {
		for i := range batch {
			if s.err != nil {
				break
			}
			if s.plan != nil {
				s.err = s.plan(&batch[i])
			}
			if s.err == nil {
				s.err = s.add(batch[i].depth, &batch[i].rec)
			}
		}
		if s.err != nil {
			s.failed.Store(true)
		}
		s.free <- batch
	}
}

// stop sends the partly filled batch, ends the writer and returns its
// error. Idempotent.
func (s *levelSpool) stop() error {
	if s.stopped {
		return s.err
	}
	s.stopped = true
	if s.started {
		if len(s.batch) > 0 {
			s.full <- s.batch
		}
		close(s.full)
		s.writer.Wait()
	}
	// A slot for any next after the stop, which commit then refuses.
	s.batch = make([]spoolEntry, 0, 1)

	return s.err
}

// spoolFile is one depth's record stream.
type spoolFile struct {
	path string
	f    *os.File
	bw   *bufio.Writer
}

// levelSpoolBufSize buffers each depth's records ahead of its file.
const levelSpoolBufSize = 256 << 10

// spoolRecord is one prefix as Finalize needs it. Child and parent are
// indexes within their own depth (the next and the previous); Finalize
// adds the depth's first row once every depth's size is known. It is
// one cache line, its tier values kept apart: the replay fills records
// on one core and writes them on two others, and a record holding every
// tier's values spanned over five.
type spoolRecord struct {
	count, bytes uint64
	childLocal   uint64 // the prefix's first child, within depth+1
	parentLocal  uint64 // the parent, within depth-1; 0 for the root
	// tiers holds the values of the tier slots in tierMask, as
	// format.TierStatsPlan.Add gives them, in a buffer the record's
	// filler owns.
	tiers    []uint64
	segID    uint32
	tierMask uint16
}

func (s *levelSpool) file(depth int) (*spoolFile, error) {
	for len(s.depths) <= depth {
		d := len(s.depths)
		path := filepath.Join(s.dir, fmt.Sprintf("level_%03d.spool", d))
		f, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("create level spool %d: %w", d, err)
		}
		s.depths = append(s.depths, &spoolFile{path: path, f: f, bw: bufio.NewWriterSize(f, levelSpoolBufSize)})
	}

	return s.depths[depth], nil
}

// add appends r to depth's records.
func (s *levelSpool) add(depth int, r *spoolRecord) error {
	sf, err := s.file(depth)
	if err != nil {
		return err
	}
	// A record is its length, then its fields, so the reader can decode
	// it from one slice of its buffer.
	s.rec = s.rec[:0]
	buf := binary.AppendUvarint(s.rec, r.count)
	buf = binary.AppendUvarint(buf, r.bytes)
	buf = binary.AppendUvarint(buf, r.childLocal)
	buf = binary.AppendUvarint(buf, r.parentLocal)
	buf = binary.AppendUvarint(buf, uint64(r.segID))
	buf = binary.AppendUvarint(buf, uint64(r.tierMask))
	for _, v := range r.tiers {
		buf = binary.AppendUvarint(buf, v)
	}
	s.rec = buf
	head := binary.AppendUvarint(sf.bw.AvailableBuffer(), uint64(len(buf)))
	if _, err := sf.bw.Write(head); err != nil {
		return fmt.Errorf("write level spool %d: %w", depth, err)
	}
	if _, err := sf.bw.Write(buf); err != nil {
		return fmt.Errorf("write level spool %d: %w", depth, err)
	}

	return nil
}

// finish writes the rows still with the writer, flushes every depth's
// stream and closes its file for reading.
func (s *levelSpool) finish() error {
	if err := s.stop(); err != nil {
		return err
	}
	for d, sf := range s.depths {
		err := sf.bw.Flush()
		if err == nil {
			err = sf.f.Close()
		} else {
			err = errors.Join(err, sf.f.Close())
		}
		sf.f = nil
		if err != nil {
			return fmt.Errorf("close level spool %d: %w", d, err)
		}
	}

	return nil
}

// spoolReader reads one depth's records in the order they were added.
type spoolReader struct {
	f     *os.File
	in    readWindow
	depth int
}

// open starts reading depth's records.
func (s *levelSpool) open(depth int) (*spoolReader, error) {
	f, err := os.Open(s.depths[depth].path)
	if err != nil {
		return nil, fmt.Errorf("open level spool %d: %w", depth, err)
	}

	return &spoolReader{f: f, in: readWindow{r: bufio.NewReaderSize(f, levelSpoolBufSize)}, depth: depth}, nil
}

// next decodes the next record into rec, its tier values appended to
// vals: io.EOF after the last.
func (r *spoolReader) next(rec *spoolRecord, vals []uint64) ([]uint64, error) {
	vals, err := r.read(rec, vals)
	if err != nil && !errors.Is(err, io.EOF) {
		return vals, fmt.Errorf("read level spool %d: %w", r.depth, err)
	}

	return vals, err
}

// read decodes the next record into rec, its tier values appended to
// vals: io.EOF at a clean end of the stream, errSpoolTruncated inside a
// record.
func (r *spoolReader) read(rec *spoolRecord, vals []uint64) ([]uint64, error) {
	b := r.in.rest()
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		if err := r.refill(); err != nil {
			return vals, err
		}
		b = r.in.rest()
		n, k = binary.Uvarint(b)
	}
	end := k + int(n)
	vals, err := decodeSpoolRecord(b[k:end], rec, vals)
	if err != nil {
		return vals, err
	}
	r.in.advance(end)

	return vals, nil
}

// refill refills the window for the record that runs past it: io.EOF
// at a clean end of the stream, errSpoolTruncated if it ends inside the
// record.
func (r *spoolReader) refill() error {
	readErr := r.in.refill(r.in.r.Size())
	b := r.in.rest()
	if len(b) == 0 {
		if errors.Is(readErr, io.EOF) {
			return io.EOF
		}

		return readErr
	}
	n, k := binary.Uvarint(b)
	switch {
	case k < 0:
		return fmt.Errorf("%w: record length overflows", errSpoolTruncated)
	case k > 0 && n <= uint64(len(b)-k):
		return nil
	case readErr == nil:
		return fmt.Errorf("%w: %d-byte record, longer than the read buffer", errSpoolTruncated, n)
	}

	return truncated(readErr)
}

func (r *spoolReader) close() error {
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("close level spool %d: %w", r.depth, err)
	}

	return nil
}

// remove deletes depth's file once it has been read.
func (s *levelSpool) remove(depth int) error {
	return removeIfExists(s.depths[depth].path)
}

// errSpoolTruncated reports a record cut short: the stream ended inside
// it.
var errSpoolTruncated = errors.New("level spool record truncated")

// decodeSpoolRecord decodes one record from buf, which holds exactly
// it, appending its tier values to vals; r.tiers is them.
func decodeSpoolRecord(buf []byte, r *spoolRecord, vals []uint64) ([]uint64, error) {
	next := func() uint64 {
		// Most fields (small counts, segment IDs, masks) fit in a byte.
		if len(buf) > 0 && buf[0] < 0x80 {
			v := buf[0]
			buf = buf[1:]

			return uint64(v)
		}
		v, k := binary.Uvarint(buf)
		if k <= 0 {
			buf = nil

			return 0
		}
		buf = buf[k:]

		return v
	}
	r.count, r.bytes, r.childLocal, r.parentLocal = next(), next(), next(), next()
	segID := next()
	if segID > math.MaxUint32 {
		return vals, fmt.Errorf("%w: segment ID %d", errSpoolTruncated, segID)
	}
	r.segID = uint32(segID)
	mask := next()
	if mask > math.MaxUint16 {
		return vals, fmt.Errorf("%w: tier mask %#x", errSpoolTruncated, mask)
	}
	r.tierMask = uint16(mask)
	start := len(vals)
	for range 2 * bits.OnesCount16(r.tierMask) {
		vals = append(vals, next())
	}
	if buf == nil || len(buf) != 0 {
		return vals[:start], errSpoolTruncated
	}
	r.tiers = vals[start:len(vals):len(vals)]

	return vals, nil
}

func truncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errSpoolTruncated
	}

	return err
}

// close stops the writer, closes every depth's file and removes it:
// after a failed build, or the depth files readLevels left behind after a
// failure. Idempotent.
func (s *levelSpool) close() error {
	errs := []error{s.stop()}
	for _, sf := range s.depths {
		if sf.f != nil {
			errs = append(errs, sf.f.Close())
			sf.f = nil
		}
		errs = append(errs, removeIfExists(sf.path))
	}
	s.depths = nil

	return errors.Join(errs...)
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", filepath.Base(path), err)
	}

	return nil
}
