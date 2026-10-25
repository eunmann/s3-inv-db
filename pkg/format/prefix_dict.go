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

// PreloadedPrefixCache holds every segment in memory for direct
// slice-index lookup, avoiding the LRU.
type PreloadedPrefixCache struct {
	segments []string
}

// PreloadSegments loads all segments into memory for fast lookups.
func (sd *PrefixDictionary) PreloadSegments() (*PreloadedPrefixCache, error) {
	count := sd.Count()
	segments := make([]string, count)
	for i := range count {
		seg, err := sd.blob.Get(i)
		if err != nil {
			return nil, fmt.Errorf("preload segment %d: %w", i, err)
		}
		segments[i] = seg
	}

	return &PreloadedPrefixCache{segments: segments}, nil
}

// Get panics if id is out of range.
func (c *PreloadedPrefixCache) Get(id uint32) string {
	return c.segments[id]
}

func (c *PreloadedPrefixCache) Count() int {
	return len(c.segments)
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
