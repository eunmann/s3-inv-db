package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// prefixTreeScratchRow is a build-time row: segment ID (4 bytes) then
// parent distance (8 bytes).
const prefixTreeScratchRow = 12

// PrefixTreeScratchWriter writes the prefix tree and its segment dictionary.
// Rows go to a fixed-width scratch file during the build; Close writes
// the final file with each field at the width its largest value needs.
type PrefixTreeScratchWriter struct {
	interner    *PrefixSegmentInterner
	scratch     *os.File
	bw          *bufio.Writer
	finalPath   string
	scratchPath string
	count       uint64
	maxDist     uint64
}

// NewPrefixTreeScratchWriter creates the writer for the index in outDir.
func NewPrefixTreeScratchWriter(outDir string) (*PrefixTreeScratchWriter, error) {
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

	return &PrefixTreeScratchWriter{
		interner:    interner,
		scratch:     f,
		bw:          bufio.NewWriterSize(f, repackBufferSize),
		finalPath:   finalPath,
		scratchPath: scratchPath,
	}, nil
}

// AddRoot adds the root prefix "", which must come first.
func (w *PrefixTreeScratchWriter) AddRoot() error {
	if w.count != 0 {
		return fmt.Errorf("%w: root at position %d", errPrefixTreeOrder, w.count)
	}

	return w.writeRow(0, 0)
}

// Add adds the next prefix: the prefix at parentPos, then name, then a
// slash. Name may alias a buffer the caller reuses.
func (w *PrefixTreeScratchWriter) Add(parentPos uint64, name string) error {
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

func (w *PrefixTreeScratchWriter) writeRow(segID uint32, dist uint64) error {
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
func (w *PrefixTreeScratchWriter) Close() error {
	err := errors.Join(w.bw.Flush(), w.scratch.Close(), w.interner.Close())
	if err == nil {
		err = w.writeTree()
	}

	return errors.Join(err, removeIfErr(w.scratchPath))
}

// Abort removes the scratch file without writing the tree; the caller
// removes the rest of the index.
func (w *PrefixTreeScratchWriter) Abort() error {
	return errors.Join(w.scratch.Close(), w.interner.Close(), removeIfErr(w.scratchPath))
}

func (w *PrefixTreeScratchWriter) writeTree() error {
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
	startWriteback(dst)
	if err := dst.Close(); err != nil {
		return errors.Join(fmt.Errorf("close prefix tree: %w", err), removeIfErr(w.finalPath))
	}

	return nil
}
