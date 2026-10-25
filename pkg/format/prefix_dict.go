package format

import (
	"fmt"
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
// to outDir.
func NewPrefixSegmentInterner(outDir string) (*PrefixSegmentInterner, error) {
	blobPath := filepath.Join(outDir, PrefixDictBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixDictOffsetsFile)

	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("create prefix dict blob writer: %w", err)
	}

	return &PrefixSegmentInterner{
		ids:        make(map[string]uint32, 1024),
		blobWriter: writer,
	}, nil
}

// Intern returns the ID for segment, assigning and writing a new one
// on first occurrence. Looking up an existing segment does not
// allocate; a new one is copied, so segment may alias a buffer the
// caller reuses.
func (si *PrefixSegmentInterner) Intern(segment string) (uint32, error) {
	if id, ok := si.ids[segment]; ok {
		return id, nil
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

func (si *PrefixSegmentInterner) Count() uint32 {
	return si.nextID
}

func (si *PrefixSegmentInterner) Close() error {
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
// whole segment blob, and each segment a substring of it. That is two
// allocations however many distinct segments an index has, not one per
// segment, and a Get is one slice index. Segments are heap strings, so
// they stay valid after the index closes.
type PreloadedPrefixCache struct {
	segments []string
	bytes    uint64
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
	data := string(blob[:end])
	segments := make([]string, n)
	prev := offsets.UnsafeGetU64(0)
	for i := range n {
		next := offsets.UnsafeGetU64(i + 1)
		if next < prev || next > end {
			return nil, fmt.Errorf("%w: segment offset %d at %d", ErrBoundsCheck, next, i+1)
		}
		segments[i] = data[prev:next]
		prev = next
	}

	return &PreloadedPrefixCache{segments: segments, bytes: end}, nil
}

// Get returns segment id. It panics if id is out of range.
func (c *PreloadedPrefixCache) Get(id uint32) string {
	return c.segments[id]
}

// Count returns the number of segments.
func (c *PreloadedPrefixCache) Count() int {
	return len(c.segments)
}

// Bytes returns the segments' total length.
func (c *PreloadedPrefixCache) Bytes() uint64 {
	return c.bytes
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
