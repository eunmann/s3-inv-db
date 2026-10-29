package format

import (
	"errors"
	"fmt"
	"path/filepath"
)

// The range writers (CoreStatsRanges, TierStatsRanges, PrefixTreeRanges,
// DepthIndexRanges) write a file whose every row has a place known up
// front from several goroutines at once, each a range of rows: the file
// is created at its final size with its header, and each goroutine's
// range writes its rows to their place through its own rangeBuf. What a
// row's place does not fix (the core patches, the tier rows' bit vector,
// the checks a sequential writer makes as it closes) each range leaves
// in a part that one goroutine merges in row order.

// rangeBufSize is one goroutine's buffer per file it writes ranges of.
// Writes to one file serialize in the kernel, so a write must carry
// enough rows that the copy, not the lock, is what they cost.
const rangeBufSize = 128 << 10

// rangeSlack is the bytes past a row a range writer may store into: each
// field goes in as a whole word over the bytes after it.
const rangeSlack = 8

// rangeBuf buffers one goroutine's writes to consecutive bytes of a file
// that others write other bytes of at the same time.
type rangeBuf struct {
	f   *hashedFile
	buf []byte
	off int64 // where buf[0] goes in the file
}

func newRangeBuf(f *hashedFile) rangeBuf {
	return rangeBuf{f: f, buf: make([]byte, 0, rangeBufSize)}
}

// seek writes what is buffered and continues at off.
func (b *rangeBuf) seek(off int64) error {
	if err := b.flush(); err != nil {
		return err
	}
	b.off = off

	return nil
}

// next returns the n bytes to fill next, with rangeSlack bytes after
// them that the next row overwrites, first writing what is buffered when
// they don't fit.
func (b *rangeBuf) next(n int) ([]byte, error) {
	if len(b.buf)+n+rangeSlack > cap(b.buf) {
		if err := b.flush(); err != nil {
			return nil, err
		}
	}
	start := len(b.buf)
	b.buf = b.buf[:start+n]

	return b.buf[start : start+n+rangeSlack], nil
}

// flush writes what is buffered to its place.
func (b *rangeBuf) flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	if _, err := b.f.WriteAt(b.buf, b.off); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(b.f.path), err)
	}
	b.off += int64(len(b.buf))
	b.buf = b.buf[:0]

	return nil
}

// createSizedFile creates the file at path at its final size, its head
// written and every other byte zero until a range fills it.
func createSizedFile(path string, head []byte, size int64, sums *Checksums) (*hashedFile, error) {
	f, err := createHashedFile(path, sums)
	if err != nil {
		return nil, err
	}
	if err := f.truncate(size); err != nil {
		return nil, errors.Join(fmt.Errorf("size %s: %w", filepath.Base(path), err), f.Close(), removeIfErr(path))
	}
	if _, err := f.WriteAt(head, 0); err != nil {
		return nil, errors.Join(fmt.Errorf("write %s header: %w", filepath.Base(path), err), f.Close(), removeIfErr(path))
	}

	return f, nil
}

// closeSized finishes a file createSizedFile made; on an error it
// removes it.
func closeSized(f *hashedFile) error {
	f.startWriteback()
	if err := f.Close(); err != nil {
		return errors.Join(fmt.Errorf("close %s: %w", filepath.Base(f.path), err), removeIfErr(f.path))
	}

	return nil
}

// abortSized removes a file createSizedFile made. Nil is a no-op.
func abortSized(f *hashedFile) error {
	if f == nil {
		return nil
	}

	return errors.Join(f.Close(), removeIfErr(f.path))
}

// errRangeOrder means a range writer's parts were merged out of row
// order, or a range wrote outside its place.
var errRangeOrder = errors.New("range writer: rows out of order")
