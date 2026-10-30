package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"
)

// Segment-dictionary file names: the segments the interner wrote, each
// distinct one once unless its budget could not hold them all. The
// prefix tree (prefix_tree.go) refers to segments by ID.
const (
	PrefixDictBlobFile    = "prefix_dict.bin"
	PrefixDictOffsetsFile = "prefix_dict.off.u64"
)

// PrefixDictionary reads segments by ID from the mapped dictionary
// files. It keeps nothing per segment in memory, so an open index costs
// the same heap however many distinct segments it has; the page cache
// holds the segments a reader touches.
type PrefixDictionary struct {
	blob *BlobReader
	// names is the mapped segment blob, offs the mapped offsets (offW
	// bytes each, read as 8 and masked), held here so a read is two
	// loads and no pointer chase.
	names []byte
	offs  []byte
	offW  uint64
	mask  uint64
	count uint64
}

// OpenPrefixDictionary opens the segment dictionary of the index in dir.
// It checks only the offsets file's shape and where the last segment
// ends, in constant time; each read checks its own segment's offsets.
func OpenPrefixDictionary(dir string) (*PrefixDictionary, error) {
	blob, err := OpenBlob(filepath.Join(dir, PrefixDictBlobFile), filepath.Join(dir, PrefixDictOffsetsFile))
	if err != nil {
		return nil, fmt.Errorf("open prefix dict blob: %w", err)
	}
	offsets := blob.offsetsMmap
	// Every offset is read as 8 bytes, so the last one needs the tail
	// pad behind it.
	if w := uint64(offsets.Width()); offsets.Count() == 0 || w < 1 || w > 8 ||
		uint64(len(offsets.data)) < (offsets.Count()-1)*w+8 {
		return nil, errors.Join(fmt.Errorf("%w: %d segment offsets of width %d in %d bytes",
			ErrBoundsCheck, offsets.Count(), w, len(offsets.data)), blob.Close())
	}
	count := offsets.Count() - 1
	names := blob.blobMmap.Data()
	if end := offsets.UnsafeGetU64(count); end > uint64(len(names)) {
		return nil, errors.Join(fmt.Errorf("%w: segment blob ends at %d of %d bytes", ErrBoundsCheck, end, len(names)), blob.Close())
	}

	w := uint64(offsets.Width())

	return &PrefixDictionary{blob: blob, names: names, offs: offsets.data, offW: w, mask: widthMask[w], count: count}, nil
}

// view returns segment id as a string over the mapped file, valid only
// until Close, or false if id is out of range or its offsets are
// corrupt. Callers copy any segment that outlives the call.
func (sd *PrefixDictionary) view(id uint32) (string, bool) {
	if uint64(id) >= sd.count {
		return "", false
	}
	i := uint64(id) * sd.offW
	start := binary.LittleEndian.Uint64(sd.offs[i:]) & sd.mask
	end := binary.LittleEndian.Uint64(sd.offs[i+sd.offW:]) & sd.mask
	if start > end || end > uint64(len(sd.names)) {
		return "", false
	}
	if start == end {
		return "", true
	}

	return unsafe.String(&sd.names[start], end-start), true
}

// GetSegment returns a copy of segment id.
func (sd *PrefixDictionary) GetSegment(id uint32) (string, error) {
	seg, ok := sd.view(id)
	if !ok {
		return "", fmt.Errorf("%w: segment %d of %d", ErrBoundsCheck, id, sd.count)
	}

	return strings.Clone(seg), nil
}

// Count returns the number of segments in the dictionary.
func (sd *PrefixDictionary) Count() uint64 {
	return sd.count
}

// Bytes returns the segments' total length.
func (sd *PrefixDictionary) Bytes() uint64 {
	offsets := sd.blob.offsetsMmap

	return offsets.UnsafeGetU64(sd.count) - offsets.UnsafeGetU64(0)
}

// Close unmaps the files. Segments from view are invalid after it.
func (sd *PrefixDictionary) Close() error {
	return sd.blob.Close()
}

// SplitPrefix splits a prefix into path segments by "/". Trailing
// empty strings are preserved to distinguish prefixes from keys.
//
// Examples:
//
//	""           → [""]
//	"data/"      → ["data", ""]
//	"data/2024/" → ["data", "2024", ""]
//	"a/b/c"      → ["a", "b", "c"]
func SplitPrefix(prefix string) []string {
	return strings.Split(prefix, "/")
}
