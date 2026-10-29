package format

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
)

// Segment-dictionary file names: every distinct segment, once. The
// prefix tree (prefix_tree.go) refers to segments by ID.
const (
	PrefixDictBlobFile    = "prefix_dict.bin"
	PrefixDictOffsetsFile = "prefix_dict.off.u64"
)

// DefaultPrefixDictCacheSize is the default LRU cache size for
// PrefixDictionary when callers don't preload.
const DefaultPrefixDictCacheSize = 10000

// PrefixSegmentInterner assigns a sequential uint32 ID to each unique
// last segment of a prefix; prefix_tree.bin rows reference these IDs.
type PrefixSegmentInterner struct {
	// ids is keyed by the segment itself: exact, so no hash collision
	// can fail a build.
	ids        map[string]uint32
	blobWriter *BlobWriter
	nextID     uint32
}

// NewPrefixSegmentInterner creates a new segment interner that writes
// to outDir, recording its files' checksums in sums.
func NewPrefixSegmentInterner(outDir string, sums *Checksums) (*PrefixSegmentInterner, error) {
	blobPath := filepath.Join(outDir, PrefixDictBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixDictOffsetsFile)

	writer, err := NewBlobWriter(blobPath, offsetsPath, sums)
	if err != nil {
		return nil, fmt.Errorf("create prefix dict blob writer: %w", err)
	}

	return &PrefixSegmentInterner{
		ids:        make(map[string]uint32, 1024),
		blobWriter: writer,
	}, nil
}

// ErrTooManySegments is returned when an index would need more
// distinct segments than a segment ID can number.
var ErrTooManySegments = errors.New("too many distinct prefix segments")

// Intern returns the ID for segment, assigning and writing a new one
// on first occurrence. Looking up an existing segment does not
// allocate; a new one is copied, so segment may alias a buffer the
// caller reuses.
func (si *PrefixSegmentInterner) Intern(segment string) (uint32, error) {
	if id, ok := si.ids[segment]; ok {
		return id, nil
	}

	if si.nextID == math.MaxUint32 {
		return 0, fmt.Errorf("%w: more than %d distinct segments", ErrTooManySegments, uint64(math.MaxUint32))
	}
	id := si.nextID
	si.nextID++
	owned := strings.Clone(segment)
	if err := si.blobWriter.WriteString(owned); err != nil {
		return 0, fmt.Errorf("write segment: %w", err)
	}
	si.ids[owned] = id

	return id, nil
}

// Count returns the number of distinct segments interned.
func (si *PrefixSegmentInterner) Count() uint32 {
	return si.nextID
}

// Close writes the dictionary's last offset and closes its files. It
// drops the segment map, which holds every distinct segment, so Count
// is all that stays usable.
func (si *PrefixSegmentInterner) Close() error {
	si.ids = nil

	return si.blobWriter.Close()
}

// PrefixDictionary provides ID→segment lookup backed by an LRU cache.
type PrefixDictionary struct {
	blob  *BlobReader
	cache *lru.Cache[uint32, string]
}

// OpenPrefixDictionary opens a prefix dictionary from disk with the
// default cache size.
func OpenPrefixDictionary(outDir string) (*PrefixDictionary, error) {
	return OpenPrefixDictionaryWithCacheSize(outDir, DefaultPrefixDictCacheSize)
}

// OpenPrefixDictionaryWithCacheSize opens a prefix dictionary with a
// custom cache size.
func OpenPrefixDictionaryWithCacheSize(outDir string, cacheSize int) (*PrefixDictionary, error) {
	blobPath := filepath.Join(outDir, PrefixDictBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixDictOffsetsFile)

	blob, err := OpenBlob(blobPath, offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("open prefix dict blob: %w", err)
	}

	cache, err := lru.New[uint32, string](cacheSize)
	if err != nil {
		blob.Close()

		return nil, fmt.Errorf("create prefix dict cache: %w", err)
	}

	return &PrefixDictionary{blob: blob, cache: cache}, nil
}

// GetSegment returns the segment string for the given ID.
func (sd *PrefixDictionary) GetSegment(id uint32) (string, error) {
	if seg, ok := sd.cache.Get(id); ok {
		return seg, nil
	}
	seg, err := sd.blob.Get(uint64(id))
	if err != nil {
		return "", err
	}
	sd.cache.Add(id, seg)

	return seg, nil
}

// UnsafeGetSegment returns the segment without bounds checking.
func (sd *PrefixDictionary) UnsafeGetSegment(id uint32) string {
	if seg, ok := sd.cache.Get(id); ok {
		return seg
	}
	seg := sd.blob.UnsafeGet(uint64(id))
	sd.cache.Add(id, seg)

	return seg
}

// Count returns the number of segments in the dictionary.
func (sd *PrefixDictionary) Count() uint64 {
	return sd.blob.Count()
}

// Close releases resources.
func (sd *PrefixDictionary) Close() error {
	return sd.blob.Close()
}

// PreloadedPrefixCache holds every segment in memory: one copy of the
// whole segment blob and the segments' offsets into it. The offsets are
// pointer-free, so the garbage collector scans none of them however
// many distinct segments an index has (a []string would hold a pointer
// per segment, each marked every cycle, at 16 bytes instead of 8).
// Segments are substrings of a heap string, so they stay valid after
// the index closes.
type PreloadedPrefixCache struct {
	data string
	offs []uint64 // segment id's bytes are data[offs[id]:offs[id+1]]
}

// PreloadSegments copies the segment blob into memory.
func (sd *PrefixDictionary) PreloadSegments() (*PreloadedPrefixCache, error) {
	n := sd.Count()
	offsets := sd.blob.offsetsMmap
	if offsets.Count() != n+1 {
		return nil, fmt.Errorf("%w: %d segment offsets for %d segments", ErrBoundsCheck, offsets.Count(), n)
	}
	end := offsets.UnsafeGetU64(n)
	blob := sd.blob.blobMmap.Data()
	if end > uint64(len(blob)) {
		return nil, fmt.Errorf("%w: segment blob ends at %d of %d bytes", ErrBoundsCheck, end, len(blob))
	}
	offs := make([]uint64, n+1)
	offs[0] = offsets.UnsafeGetU64(0)
	for i := range n {
		next := offsets.UnsafeGetU64(i + 1)
		if next < offs[i] || next > end {
			return nil, fmt.Errorf("%w: segment offset %d at %d", ErrBoundsCheck, next, i+1)
		}
		offs[i+1] = next
	}

	return &PreloadedPrefixCache{data: string(blob[:end]), offs: offs}, nil
}

// Get returns segment id. It panics if id is out of range.
func (c *PreloadedPrefixCache) Get(id uint32) string {
	return c.data[c.offs[id]:c.offs[id+1]]
}

// Count returns the number of segments.
func (c *PreloadedPrefixCache) Count() int {
	return len(c.offs) - 1
}

// Bytes returns the segments' total length.
func (c *PreloadedPrefixCache) Bytes() uint64 {
	return uint64(len(c.data))
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
