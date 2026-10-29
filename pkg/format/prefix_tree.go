package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"unsafe"
)

// PrefixTreeFile maps each position to its prefix string.
const PrefixTreeFile = "prefix_tree.bin"

// The prefix tree stores, for each prefix in position (level) order,
// the dictionary ID of its last segment and the distance back to its
// parent's position, a row of the level above. A prefix is its parent, then that segment, then a slash
// ("data/2024/" is "data/" + "2024" + "/"), so the tree rebuilds a
// prefix with one row per level and names a child with one row. The
// root "" is position 0 and stores nothing (its row is zero).
//
// File layout: Header (Count = prefixes, Width = row bytes), a 2-byte
// descriptor (segment ID width, parent distance width), the rows, an
// 8-byte zero tail pad.
const (
	prefixTreeDescBytes = 2
	prefixTreeDataOff   = HeaderSize + prefixTreeDescBytes
	prefixTreeTailPad   = 8
	// PrefixTreeMaxIDWidth is the widest segment ID: IDs are uint32.
	prefixTreeMaxIDWidth = 4
)

var (
	errPrefixTreeOrder = errors.New("prefix tree: rows out of order")
	errPrefixTreeFile  = errors.New("prefix tree: invalid file")
)

// errPrefixTreePlan means the rows given to a prefix tree writer differ
// from the count, segments and parent distances it was made for.
var errPrefixTreePlan = errors.New("prefix tree: rows differ from their plan")

// PrefixTreeWriter writes the prefix tree row by row in position order,
// straight to its final widths: the caller gives the row count, the
// number of distinct segments (interned by a PrefixSegmentInterner,
// which writes the dictionary) and the largest parent distance up front.
type PrefixTreeWriter struct {
	path     string
	f        *os.File
	bw       *bufio.Writer
	count    uint64
	segments uint32
	maxDist  uint64
	// The rows written, and their largest segment ID and distance.
	n              uint64
	topSeg         uint32
	topDist        uint64
	segW, distW    uint8
	closed, anySeg bool
	row            [16]byte
}

// NewPrefixTreeWriter creates the prefix tree of count prefixes in
// outDir, whose segment IDs are below segments and whose parents are at
// most maxDist positions back.
func NewPrefixTreeWriter(outDir string, count uint64, segments uint32, maxDist uint64) (*PrefixTreeWriter, error) {
	w := &PrefixTreeWriter{
		path:     filepath.Join(outDir, PrefixTreeFile),
		count:    count,
		segments: segments,
		maxDist:  maxDist,
		segW:     byteWidthOf(uint64(max(segments, 1) - 1)),
		distW:    byteWidthOf(maxDist),
	}
	f, err := os.Create(w.path)
	if err != nil {
		return nil, fmt.Errorf("create prefix tree: %w", err)
	}
	w.f, w.bw = f, bufio.NewWriterSize(f, writeBufSize)
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: count, Width: uint32(w.segW) + uint32(w.distW)})
	if _, err := w.bw.Write(append(head, w.segW, w.distW)); err != nil {
		return nil, errors.Join(fmt.Errorf("write prefix tree header: %w", err), w.Abort())
	}

	return w, nil
}

// AddRoot adds the root prefix "", which must come first.
func (w *PrefixTreeWriter) AddRoot() error {
	if w.n != 0 {
		return fmt.Errorf("%w: root at position %d", errPrefixTreeOrder, w.n)
	}

	return w.writeRow(0, 0)
}

// Add adds the next prefix: the prefix at parentPos, then segment segID,
// then a slash.
func (w *PrefixTreeWriter) Add(parentPos uint64, segID uint32) error {
	if w.n == 0 || parentPos >= w.n {
		return fmt.Errorf("%w: parent %d for position %d", errPrefixTreeOrder, parentPos, w.n)
	}
	dist := w.n - parentPos
	if segID >= w.segments || dist > w.maxDist {
		return fmt.Errorf("%w: segment %d of %d, parent distance %d of at most %d",
			errPrefixTreePlan, segID, w.segments, dist, w.maxDist)
	}
	w.topSeg, w.topDist, w.anySeg = max(w.topSeg, segID), max(w.topDist, dist), true

	return w.writeRow(segID, dist)
}

func (w *PrefixTreeWriter) writeRow(segID uint32, dist uint64) error {
	if w.n == w.count {
		return fmt.Errorf("%w: more than %d rows", errPrefixTreePlan, w.count)
	}
	// Each field goes in as a whole word over the bytes after it.
	binary.LittleEndian.PutUint64(w.row[:], uint64(segID))
	binary.LittleEndian.PutUint64(w.row[w.segW:], dist)
	if _, err := w.bw.Write(w.row[:w.segW+w.distW]); err != nil {
		return fmt.Errorf("write prefix tree: %w", err)
	}
	w.n++

	return nil
}

// Close finishes the file once every row is written and the largest
// segment ID and distance are the ones the widths were chosen for. On
// an error it removes the file.
func (w *PrefixTreeWriter) Close() error {
	segments := uint32(0)
	if w.anySeg {
		segments = w.topSeg + 1
	}
	if w.n != w.count || segments != w.segments || w.topDist != w.maxDist {
		return errors.Join(fmt.Errorf("%w: %d rows, %d segments, parent distance %d; planned %d, %d, %d",
			errPrefixTreePlan, w.n, segments, w.topDist, w.count, w.segments, w.maxDist), w.Abort())
	}
	var pad [prefixTreeTailPad]byte
	if _, err := w.bw.Write(pad[:]); err != nil {
		return errors.Join(fmt.Errorf("write prefix tree tail pad: %w", err), w.Abort())
	}
	if err := w.bw.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush prefix tree: %w", err), w.Abort())
	}
	startWriteback(w.f)
	err := w.f.Close()
	w.f = nil
	if err != nil {
		return errors.Join(fmt.Errorf("close prefix tree: %w", err), w.Abort())
	}
	w.closed = true

	return nil
}

// Abort removes the file unless Close finished it. Idempotent.
func (w *PrefixTreeWriter) Abort() error {
	if w.closed {
		return nil
	}
	var err error
	if w.f != nil {
		err = w.f.Close()
		w.f = nil
	}

	return errors.Join(err, removeIfErr(w.path))
}

// PrefixTreeReader maps positions to prefix strings. Segments are
// preloaded into memory, so naming a child is one row read and
// rebuilding a prefix is one row read per level.
type PrefixTreeReader struct {
	dict     *PrefixDictionary
	cache    *PreloadedPrefixCache
	tree     *MmapFile
	count    uint64
	rowWidth uint64
	segW     uint8
	distW    uint8
}

// OpenPrefixTreeReader opens the prefix tree of the index in dir.
func OpenPrefixTreeReader(dir string) (*PrefixTreeReader, error) {
	dict, err := OpenPrefixDictionary(dir)
	if err != nil {
		return nil, fmt.Errorf("open prefix dictionary: %w", err)
	}
	cache, err := dict.PreloadSegments()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("preload segments: %w", err), dict.Close())
	}
	tree, err := OpenMmap(filepath.Join(dir, PrefixTreeFile))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open prefix tree: %w", err), dict.Close())
	}
	r := &PrefixTreeReader{dict: dict, cache: cache, tree: tree}
	if err := r.checkTree(); err != nil {
		return nil, errors.Join(err, r.Close())
	}

	return r, nil
}

func (r *PrefixTreeReader) checkTree() error {
	data := r.tree.Data()
	if len(data) < prefixTreeDataOff {
		return fmt.Errorf("%w: %d bytes", errPrefixTreeFile, len(data))
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return fmt.Errorf("decode prefix tree header: %w", err)
	}
	if header.Magic != MagicNumber {
		return ErrMagicMismatch
	}
	if header.Version != Version {
		return ErrVersionMismatch
	}
	r.segW, r.distW = data[HeaderSize], data[HeaderSize+1]
	if r.segW < 1 || r.segW > prefixTreeMaxIDWidth || r.distW < 1 || r.distW > 8 ||
		header.Width != uint32(r.segW)+uint32(r.distW) {
		return fmt.Errorf("%w: widths %d+%d, row %d", errPrefixTreeFile, r.segW, r.distW, header.Width)
	}
	r.count, r.rowWidth = header.Count, uint64(header.Width)
	if want := uint64(prefixTreeDataOff) + r.count*r.rowWidth + prefixTreeTailPad; uint64(len(data)) < want {
		return fmt.Errorf("%w: %d < %d", ErrFileTooSmall, len(data), want)
	}

	return nil
}

// row returns pos's segment ID and parent distance. No bounds check.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *PrefixTreeReader) row(pos uint64) (uint32, uint64) {
	off := prefixTreeDataOff + pos*r.rowWidth
	data := r.tree.Data()
	seg := uint32(binary.LittleEndian.Uint64(data[off:]) & widthMask[r.segW])
	dist := binary.LittleEndian.Uint64(data[off+uint64(r.segW):]) & widthMask[r.distW]

	return seg, dist
}

// Prefix returns the prefix string at pos. It walks to the root once
// and builds the string in one allocation.
func (r *PrefixTreeReader) Prefix(pos uint64) (string, error) {
	if pos >= r.count {
		return "", ErrBoundsCheck
	}
	var stack [32]uint32
	ids := stack[:0]
	size := 0
	for p := pos; p != 0; {
		seg, dist := r.row(p)
		// Each step moves strictly closer to the root, so a corrupt
		// file can't loop.
		if dist == 0 || dist > p {
			return "", fmt.Errorf("%w: parent distance %d at %d", errPrefixTreeFile, dist, p)
		}
		ids = append(ids, seg)
		size += len(r.cache.Get(seg)) + 1
		p -= dist
	}
	if size == 0 {
		return "", nil
	}
	buf := make([]byte, 0, size)
	for _, id := range slices.Backward(ids) {
		buf = append(buf, r.cache.Get(id)...)
		buf = append(buf, '/')
	}

	return unsafe.String(unsafe.SliceData(buf), len(buf)), nil
}

// ChildSegment returns the name a listing shows for the prefix at pos:
// its last segment before the trailing slash ("2024" for "data/2024/").
// It reads one row and does not allocate. The root and out-of-range
// positions return "".
func (r *PrefixTreeReader) ChildSegment(pos uint64) string {
	if pos == 0 || pos >= r.count {
		return ""
	}
	seg, _ := r.row(pos)

	return r.cache.Get(seg)
}

// Count returns the number of prefixes.
func (r *PrefixTreeReader) Count() uint64 { return r.count }

// SegmentStats describes the segment dictionary.
type SegmentStats struct {
	// Count is the number of distinct segments, Bytes their total length.
	Count uint64
	Bytes uint64
}

// SegmentStats returns the size of the segment dictionary.
func (r *PrefixTreeReader) SegmentStats() SegmentStats {
	return SegmentStats{Count: uint64(r.cache.Count()), Bytes: r.cache.Bytes()}
}

// Close releases the mapped files.
func (r *PrefixTreeReader) Close() error {
	if r == nil {
		return nil
	}

	return errors.Join(r.dict.Close(), r.tree.Close())
}
