package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	// PrefixTreeScratchRow is a build-time row: segment ID (4 bytes)
	// then parent distance (8 bytes).
	prefixTreeScratchRow = 12
	// PrefixTreeMaxIDWidth is the widest segment ID: IDs are uint32.
	prefixTreeMaxIDWidth = 4
)

var (
	errPrefixTreeOrder = errors.New("prefix tree: rows out of order")
	errPrefixTreeFile  = errors.New("prefix tree: invalid file")
)

// PrefixTreeWriter writes the prefix tree and its segment dictionary.
// Rows go to a fixed-width scratch file during the build; Close writes
// the final file with each field at the width its largest value needs.
type PrefixTreeWriter struct {
	interner    *PrefixSegmentInterner
	scratch     *os.File
	bw          *bufio.Writer
	finalPath   string
	scratchPath string
	count       uint64
	maxDist     uint64
}

// NewPrefixTreeWriter creates the writer for the index in outDir.
func NewPrefixTreeWriter(outDir string) (*PrefixTreeWriter, error) {
	interner, err := NewPrefixSegmentInterner(outDir)
	if err != nil {
		return nil, fmt.Errorf("create prefix segment interner: %w", err)
	}
	finalPath := filepath.Join(outDir, PrefixTreeFile)
	scratchPath := finalPath + ".wide"
	f, err := os.Create(scratchPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create prefix tree scratch: %w", err), interner.Close())
	}

	return &PrefixTreeWriter{
		interner:    interner,
		scratch:     f,
		bw:          bufio.NewWriterSize(f, repackBufferSize),
		finalPath:   finalPath,
		scratchPath: scratchPath,
	}, nil
}

// AddRoot adds the root prefix "", which must come first.
func (w *PrefixTreeWriter) AddRoot() error {
	if w.count != 0 {
		return fmt.Errorf("%w: root at position %d", errPrefixTreeOrder, w.count)
	}

	return w.writeRow(0, 0)
}

// Add adds the next prefix: the prefix at parentPos, then name, then a
// slash. Name may alias a buffer the caller reuses.
func (w *PrefixTreeWriter) Add(parentPos uint64, name string) error {
	if w.count == 0 || parentPos >= w.count {
		return fmt.Errorf("%w: parent %d for position %d", errPrefixTreeOrder, parentPos, w.count)
	}
	id, err := w.interner.Intern(name)
	if err != nil {
		return fmt.Errorf("intern segment %q: %w", name, err)
	}
	dist := w.count - parentPos
	w.maxDist = max(w.maxDist, dist)

	return w.writeRow(id, dist)
}

func (w *PrefixTreeWriter) writeRow(segID uint32, dist uint64) error {
	// Encode into the writer's free space: a local array passed to Write
	// would escape, one allocation per prefix.
	buf := binary.LittleEndian.AppendUint32(w.bw.AvailableBuffer(), segID)
	buf = binary.LittleEndian.AppendUint64(buf, dist)
	if _, err := w.bw.Write(buf); err != nil {
		return fmt.Errorf("write prefix tree scratch: %w", err)
	}
	w.count++

	return nil
}

// Close writes the final tree file and the segment dictionary and
// removes the scratch file.
func (w *PrefixTreeWriter) Close() error {
	err := errors.Join(w.bw.Flush(), w.scratch.Close(), w.interner.Close())
	if err == nil {
		err = w.writeTree()
	}

	return errors.Join(err, removeIfErr(w.scratchPath))
}

// Abort removes the scratch file without writing the tree; the caller
// removes the rest of the index.
func (w *PrefixTreeWriter) Abort() error {
	return errors.Join(w.scratch.Close(), w.interner.Close(), removeIfErr(w.scratchPath))
}

func (w *PrefixTreeWriter) writeTree() error {
	segW := byteWidthOf(uint64(max(w.interner.Count(), 1) - 1))
	distW := byteWidthOf(w.maxDist)
	src, err := os.Open(w.scratchPath)
	if err != nil {
		return fmt.Errorf("open prefix tree scratch: %w", err)
	}
	defer src.Close()
	dst, err := os.Create(w.finalPath)
	if err != nil {
		return fmt.Errorf("create prefix tree: %w", err)
	}
	fail := func(err error) error {
		return errors.Join(fmt.Errorf("write prefix tree: %w", err), dst.Close(), removeIfErr(w.finalPath))
	}
	out := bufio.NewWriterSize(dst, repackBufferSize)
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: w.count, Width: uint32(segW) + uint32(distW)})
	if _, err := out.Write(append(head, segW, distW)); err != nil {
		return fail(err)
	}
	in := bufio.NewReaderSize(src, repackBufferSize)
	var row [prefixTreeScratchRow]byte
	for range w.count {
		if _, err := io.ReadFull(in, row[:]); err != nil {
			return fail(err)
		}
		if _, err := out.Write(row[:segW]); err != nil {
			return fail(err)
		}
		if _, err := out.Write(row[4 : 4+distW]); err != nil {
			return fail(err)
		}
	}
	var pad [prefixTreeTailPad]byte
	if _, err := out.Write(pad[:]); err != nil {
		return fail(err)
	}
	if err := out.Flush(); err != nil {
		return fail(err)
	}
	if err := dst.Close(); err != nil {
		return errors.Join(fmt.Errorf("close prefix tree: %w", err), removeIfErr(w.finalPath))
	}

	return nil
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
