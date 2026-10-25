package extsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// levelSpool holds the build's prefixes by depth until Finalize. Rows
// arrive in key order (preorder) but the index stores them in level
// order: every depth-0 row, then every depth-1 row, each depth in key
// order. Each depth's records stream uncompressed to their own file
// through a levelSpoolBufSize bufio writer; Finalize reads the depths
// back in turn and writes the index files in level order, removing
// each depth's file once read. Memory is one buffer per depth,
// whatever the prefix count.
type levelSpool struct {
	dir    string
	depths []*spoolFile // index = depth
	rec    []byte       // the record add is encoding
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
// adds the depth's first row once every depth's size is known.
type spoolRecord struct {
	count, bytes uint64
	childLocal   uint64 // the prefix's first child, within depth+1
	parentLocal  uint64 // the parent, within depth-1; 0 for the root
	keyHash, fp  uint64 // the MPHF key hash and fingerprint
	name         []byte // the last segment, without its slash; empty for the root
	tierCounts   [tiers.NumTiers]uint64
	tierBytes    [tiers.NumTiers]uint64
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
	var mask uint64
	for i, c := range r.tierCounts {
		if c != 0 || r.tierBytes[i] != 0 {
			mask |= 1 << i
		}
	}
	// A record is its length, then its fields, so the reader can decode
	// it from one slice of its buffer.
	s.rec = s.rec[:0]
	buf := binary.AppendUvarint(s.rec, r.count)
	buf = binary.AppendUvarint(buf, r.bytes)
	buf = binary.AppendUvarint(buf, r.childLocal)
	buf = binary.AppendUvarint(buf, r.parentLocal)
	buf = binary.LittleEndian.AppendUint64(buf, r.keyHash)
	buf = binary.LittleEndian.AppendUint64(buf, r.fp)
	buf = binary.AppendUvarint(buf, uint64(len(r.name)))
	buf = append(buf, r.name...)
	buf = binary.AppendUvarint(buf, mask)
	for m := mask; m != 0; m &= m - 1 {
		i := bits.TrailingZeros64(m)
		buf = binary.AppendUvarint(buf, r.tierCounts[i])
		buf = binary.AppendUvarint(buf, r.tierBytes[i])
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

// finish flushes every depth's stream and closes its file for reading.
func (s *levelSpool) finish() error {
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

// read calls fn with each of depth's records in the order they were
// added, then removes the depth's file. The record is reused between
// calls.
func (s *levelSpool) read(depth int, fn func(r *spoolRecord) error) error {
	sf := s.depths[depth]
	f, err := os.Open(sf.path)
	if err != nil {
		return fmt.Errorf("open level spool %d: %w", depth, err)
	}
	defer f.Close()
	in := bufio.NewReaderSize(f, levelSpoolBufSize)
	var r spoolRecord
	for {
		err := readSpoolRecord(in, &r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read level spool %d: %w", depth, err)
		}
		if err := fn(&r); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close level spool %d: %w", depth, err)
	}

	return removeIfExists(sf.path)
}

// errSpoolTruncated reports a record cut short: the stream ended inside
// it.
var errSpoolTruncated = errors.New("level spool record truncated")

// readSpoolRecord decodes the next record into r: io.EOF at a clean end
// of the stream, errSpoolTruncated inside a record.
func readSpoolRecord(in *bufio.Reader, r *spoolRecord) error {
	n, err := binary.ReadUvarint(in)
	if err != nil {
		return err //nolint:wrapcheck // io.EOF marks the clean end; the caller wraps the rest
	}
	if n > uint64(in.Size()) {
		return fmt.Errorf("%w: %d-byte record", errSpoolTruncated, n)
	}
	buf, err := in.Peek(int(n))
	if err != nil {
		return truncated(err)
	}
	if err := decodeSpoolRecord(buf, r); err != nil {
		return err
	}
	_, err = in.Discard(int(n))

	return err //nolint:wrapcheck // Discard of peeked bytes cannot fail
}

// decodeSpoolRecord decodes one record from buf, which holds exactly
// it.
func decodeSpoolRecord(buf []byte, r *spoolRecord) error {
	next := func() uint64 {
		v, k := binary.Uvarint(buf)
		if k <= 0 {
			buf = nil

			return 0
		}
		buf = buf[k:]

		return v
	}
	r.count, r.bytes, r.childLocal, r.parentLocal = next(), next(), next(), next()
	if len(buf) < 16 {
		return errSpoolTruncated
	}
	r.keyHash = binary.LittleEndian.Uint64(buf)
	r.fp = binary.LittleEndian.Uint64(buf[8:])
	buf = buf[16:]
	n := next()
	if uint64(len(buf)) < n {
		return errSpoolTruncated
	}
	r.name = append(r.name[:0], buf[:n]...)
	buf = buf[n:]
	mask := next()
	r.tierCounts, r.tierBytes = [tiers.NumTiers]uint64{}, [tiers.NumTiers]uint64{}
	for m := mask; m != 0; m &= m - 1 {
		i := bits.TrailingZeros64(m)
		if i >= int(tiers.NumTiers) {
			return fmt.Errorf("%w: tier %d", errSpoolTruncated, i)
		}
		r.tierCounts[i], r.tierBytes[i] = next(), next()
	}
	if buf == nil || len(buf) != 0 {
		return errSpoolTruncated
	}

	return nil
}

func truncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errSpoolTruncated
	}

	return err
}

// close closes every depth's file and removes it: after a
// failed build, or what read left behind. Idempotent.
func (s *levelSpool) close() error {
	var errs []error
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
