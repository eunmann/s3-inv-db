package format

import (
	"cmp"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// Checksums holds the manifest checksums of an index's files, each
// recorded by the hashedFile that wrote the file as it closed, so
// WriteManifest re-reads only the files it holds none for. It is safe
// for concurrent use; a nil *Checksums records nothing.
type Checksums struct {
	files map[string]fileChecksum
	piece int64
	mu    sync.Mutex
}

// fileChecksum is a file's checksum and the file it was taken of.
type fileChecksum struct {
	info os.FileInfo
	sum  string
}

// NewChecksums returns an empty set of checksums at the manifest's
// piece size.
func NewChecksums() *Checksums { return newChecksums(checksumPiece) }

func newChecksums(piece int64) *Checksums {
	return &Checksums{files: make(map[string]fileChecksum), piece: piece}
}

func (c *Checksums) record(path, sum string, info os.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files[filepath.Clean(path)] = fileChecksum{info: info, sum: sum}
}

// forget drops path's checksum, if any.
func (c *Checksums) forget(path string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.files, filepath.Clean(path))
}

// move follows a rename of from to to.
func (c *Checksums) move(from, to string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	from, to = filepath.Clean(from), filepath.Clean(to)
	fc, ok := c.files[from]
	delete(c.files, from)
	delete(c.files, to)
	if ok {
		c.files[to] = fc
	}
}

// lookup returns path's checksum if info, the file now at path, is the
// one it was taken of: the same file, size and modification time. A
// file written again by anything but its hashedFile is not.
func (c *Checksums) lookup(path string, info os.FileInfo) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	fc, ok := c.files[filepath.Clean(path)]
	c.mu.Unlock()
	if !ok || !os.SameFile(fc.info, info) || fc.info.Size() != info.Size() || !fc.info.ModTime().Equal(info.ModTime()) {
		return "", false
	}

	return fc.sum, true
}

// crcRange is the CRC-32C of a file's bytes [off, off+n), all within
// one checksum piece.
type crcRange struct {
	off, n int64
	crc    uint32
}

// hashedFile is an index file being written. Every write goes through
// it, at the sequential offset or at a given one, and is CRC-32C'd as
// it goes out, so Close records the file's manifest checksum without
// reading the file back. Bytes never written are zeros: the file starts
// empty, and a hole a seek or truncate leaves reads as zeros. Writes
// that overlap leave no checksum, and the manifest re-reads the file.
type hashedFile struct {
	file   *os.File
	sums   *Checksums
	path   string
	ranges []crcRange
	pos    int64 // the sequential offset
	size   int64
	mu     sync.Mutex
	// overwritten is set when bytes were written twice or cut off.
	overwritten bool
}

// createHashedFile creates (or truncates) the file at path, whose
// checksum it records in sums.
func createHashedFile(path string, sums *Checksums) (*hashedFile, error) {
	sums.forget(path)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}

	return &hashedFile{file: f, sums: sums, path: path}, nil
}

// Write writes p at the sequential offset.
func (f *hashedFile) Write(p []byte) (int, error) {
	n, err := f.file.Write(p)
	f.hash(p[:n], f.pos)
	f.pos += int64(n)
	if err != nil {
		return n, fmt.Errorf("index file: %w", err)
	}

	return n, nil
}

// WriteAt writes p at off. Writes at disjoint offsets may run at once.
func (f *hashedFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.file.WriteAt(p, off)
	f.hash(p[:n], off)
	if err != nil {
		return n, fmt.Errorf("index file: %w", err)
	}

	return n, nil
}

// seek moves the sequential offset to off; the bytes it skips stay
// zeros until a WriteAt fills them.
func (f *hashedFile) seek(off int64) error {
	if _, err := f.file.Seek(off, io.SeekStart); err != nil {
		return fmt.Errorf("index file: %w", err)
	}
	f.pos = off

	return nil
}

// truncate sets the file's size; growing it adds zeros.
func (f *hashedFile) truncate(size int64) error {
	if err := f.file.Truncate(size); err != nil {
		return fmt.Errorf("index file: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if size < f.size {
		f.overwritten = true
	}
	f.size = size

	return nil
}

// hash records p, written at off, piece by piece. The CRC runs outside
// the lock, so writers at disjoint offsets hash in parallel.
func (f *hashedFile) hash(p []byte, off int64) {
	if f.sums == nil {
		return
	}
	piece := f.sums.piece
	table := castagnoli()
	for len(p) > 0 {
		n := min(int64(len(p)), piece-off%piece)
		f.add(crcRange{off: off, n: n, crc: crc32.Checksum(p[:n], table)})
		p, off = p[n:], off+n
	}
}

// add records r, merged into the last range when it continues it within
// a piece: a sequential writer keeps one range per piece.
func (f *hashedFile) add(r crcRange) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.size = max(f.size, r.off+r.n)
	if last := len(f.ranges) - 1; last >= 0 {
		l := &f.ranges[last]
		if l.off+l.n == r.off && r.off%f.sums.piece != 0 {
			l.crc = crc32cCombine(l.crc, r.crc, r.n)
			l.n += r.n

			return
		}
	}
	f.ranges = append(f.ranges, r)
}

// Close records the file's checksum, when every byte was written once,
// and closes it.
func (f *hashedFile) Close() error {
	if f.sums != nil {
		f.record()
	}
	if err := f.file.Close(); err != nil {
		return fmt.Errorf("index file: %w", err)
	}

	return nil
}

func (f *hashedFile) record() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.overwritten {
		return
	}
	pieces, ok := joinRanges(f.ranges, f.size, f.sums.piece)
	if !ok {
		return
	}
	info, err := f.file.Stat()
	if err != nil || info.Size() != f.size {
		return
	}
	f.sums.record(f.path, joinPieces(pieces), info)
}

// joinRanges returns the CRC-32C of each piece of a file of size bytes
// whose written bytes are ranges, every other byte zero. It is false if
// two ranges overlap.
func joinRanges(ranges []crcRange, size, piece int64) ([]uint32, bool) {
	slices.SortFunc(ranges, func(a, b crcRange) int { return cmp.Compare(a.off, b.off) })
	pieces := make([]uint32, 0, max(1, (size+piece-1)/piece))
	var at int64 // bytes [0, at) are in pieces or crc
	var crc uint32
	extend := func(c uint32, n int64) {
		crc = crc32cCombine(crc, c, n)
		at += n
		if at%piece == 0 {
			pieces, crc = append(pieces, crc), 0
		}
	}
	zerosTo := func(end int64) {
		for at < end {
			n := min(end-at, piece-at%piece)
			extend(crc32cZeros(n), n)
		}
	}
	for _, r := range ranges {
		if r.off < at {
			return nil, false
		}
		zerosTo(r.off)
		extend(r.crc, r.n)
	}
	zerosTo(size)
	if size == 0 || at%piece != 0 {
		pieces = append(pieces, crc)
	}

	return pieces, true
}
