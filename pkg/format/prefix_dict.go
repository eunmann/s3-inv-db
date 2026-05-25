package format

import (
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

// Prefix-dictionary file names.
const (
	PrefixDictBlobFile             = "prefix_dict.bin"
	PrefixDictOffsetsFile          = "prefix_dict.off.u64"
	PrefixDictIDsFile              = "prefix_dict.ids.u32"
	PrefixDictOffsetsPerPrefixFile = "prefix_dict.prefix_off.u64"
)

// DefaultPrefixDictCacheSize is the default LRU cache size for
// PrefixDictionary when callers don't preload.
const DefaultPrefixDictCacheSize = 10000

// avgSegmentBytes presizes the strings.Builder when reconstructing
// a prefix (~12 bytes per segment + slash).
const avgSegmentBytes = 12

// PrefixSegmentInterner assigns a sequential uint32 ID to each unique
// path segment. The trailing empty segment after a "/" is preserved
// so prefixes ("data/") stay distinguishable from keys ("data").
type PrefixSegmentInterner struct {
	segmentMap map[uint64]uint32
	blobWriter *BlobWriter
	segments   []string
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
		segmentMap: make(map[uint64]uint32),
		segments:   make([]string, 0, 1024),
		blobWriter: writer,
		nextID:     0,
	}, nil
}

// Intern returns the ID for segment, assigning and writing a new one
// on first occurrence.
func (si *PrefixSegmentInterner) Intern(segment string) (uint32, error) {
	h := hashSegment(segment)

	if id, ok := si.segmentMap[h]; ok {
		if si.segments[id] != segment {
			return 0, fmt.Errorf("%w between %q and %q", ErrHashCollision, si.segments[id], segment)
		}

		return id, nil
	}

	id := si.nextID
	si.nextID++

	if err := si.blobWriter.WriteString(segment); err != nil {
		return 0, fmt.Errorf("write segment: %w", err)
	}

	si.segmentMap[h] = id
	si.segments = append(si.segments, segment)

	return id, nil
}

// Count returns the number of unique segments interned.
func (si *PrefixSegmentInterner) Count() uint32 {
	return si.nextID
}

// Close finalizes the segment blob file.
func (si *PrefixSegmentInterner) Close() error {
	return si.blobWriter.Close()
}

// hashSegment computes an FNV hash for segment deduplication.
func hashSegment(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))

	return h.Sum64()
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

// Get returns the segment string for the given ID. Panics if id is
// out of range.
func (c *PreloadedPrefixCache) Get(id uint32) string {
	return c.segments[id]
}

// Count returns the number of segments in the cache.
func (c *PreloadedPrefixCache) Count() int {
	return len(c.segments)
}

// DictPrefixWriter writes prefixes as sequences of dictionary segment IDs.
type DictPrefixWriter struct {
	interner      *PrefixSegmentInterner
	segIDsWriter  *ArrayWriter
	offsetsWriter *ArrayWriter
	currentOffset uint64
}

// NewDictPrefixWriter creates a writer for dictionary-encoded prefix
// storage.
func NewDictPrefixWriter(outDir string) (*DictPrefixWriter, error) {
	interner, err := NewPrefixSegmentInterner(outDir)
	if err != nil {
		return nil, fmt.Errorf("create prefix segment interner: %w", err)
	}

	segIDsPath := filepath.Join(outDir, PrefixDictIDsFile)
	segIDsWriter, err := NewArrayWriter(segIDsPath, 4)
	if err != nil {
		interner.Close()

		return nil, fmt.Errorf("create dict IDs writer: %w", err)
	}

	offsetsPath := filepath.Join(outDir, PrefixDictOffsetsPerPrefixFile)
	offsetsWriter, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		interner.Close()
		segIDsWriter.Close()

		return nil, fmt.Errorf("create dict offsets writer: %w", err)
	}

	return &DictPrefixWriter{
		interner:      interner,
		segIDsWriter:  segIDsWriter,
		offsetsWriter: offsetsWriter,
		currentOffset: 0,
	}, nil
}

// WritePrefix splits a prefix into segments, interns each segment,
// and writes the segment IDs to the output files.
func (w *DictPrefixWriter) WritePrefix(prefix string) error {
	if err := w.offsetsWriter.WriteU64(w.currentOffset); err != nil {
		return fmt.Errorf("write prefix offset: %w", err)
	}

	segments := SplitPrefix(prefix)

	for _, seg := range segments {
		id, err := w.interner.Intern(seg)
		if err != nil {
			return fmt.Errorf("intern segment %q: %w", seg, err)
		}
		if err := w.segIDsWriter.WriteU32(id); err != nil {
			return fmt.Errorf("write segment ID: %w", err)
		}
		w.currentOffset++
	}

	return nil
}

// Close finalizes all output files, writing the sentinel offset.
func (w *DictPrefixWriter) Close() error {
	if err := w.offsetsWriter.WriteU64(w.currentOffset); err != nil {
		return fmt.Errorf("write sentinel offset: %w", err)
	}

	var errs []error
	if err := w.interner.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close interner: %w", err))
	}
	if err := w.segIDsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dict IDs: %w", err))
	}
	if err := w.offsetsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close offsets: %w", err))
	}

	return errors.Join(errs...)
}

// SegmentCount returns the number of unique segments interned so far.
func (w *DictPrefixWriter) SegmentCount() uint32 {
	return w.interner.Count()
}

// PrefixCount returns the number of prefixes written so far.
func (w *DictPrefixWriter) PrefixCount() uint64 {
	return w.offsetsWriter.Count()
}

// DictPrefixReader reads prefixes from dictionary-encoded storage.
type DictPrefixReader struct {
	dict     *PrefixDictionary
	cache    *PreloadedPrefixCache
	segIDs   *ArrayReader
	offsets  *ArrayReader
	builders sync.Pool
}

// OpenDictPrefixReader opens a dictionary-encoded prefix reader from
// disk. Segments are preloaded into memory for fast lookups.
func OpenDictPrefixReader(outDir string) (*DictPrefixReader, error) {
	dict, err := OpenPrefixDictionary(outDir)
	if err != nil {
		return nil, fmt.Errorf("open prefix dictionary: %w", err)
	}

	cache, err := dict.PreloadSegments()
	if err != nil {
		dict.Close()

		return nil, fmt.Errorf("preload segments: %w", err)
	}

	segIDsPath := filepath.Join(outDir, PrefixDictIDsFile)
	segIDs, err := OpenArray(segIDsPath)
	if err != nil {
		dict.Close()

		return nil, fmt.Errorf("open dict IDs: %w", err)
	}

	offsetsPath := filepath.Join(outDir, PrefixDictOffsetsPerPrefixFile)
	offsets, err := OpenArray(offsetsPath)
	if err != nil {
		dict.Close()
		segIDs.Close()

		return nil, fmt.Errorf("open offsets: %w", err)
	}

	return &DictPrefixReader{
		dict:     dict,
		cache:    cache,
		segIDs:   segIDs,
		offsets:  offsets,
		builders: sync.Pool{New: func() any { return &strings.Builder{} }},
	}, nil
}

// getBuilder fetches a reset strings.Builder from the per-reader pool.
func (r *DictPrefixReader) getBuilder() *strings.Builder {
	b, _ := r.builders.Get().(*strings.Builder)
	if b == nil {
		b = &strings.Builder{}
	}
	b.Reset()

	return b
}

// putBuilder returns a strings.Builder to the per-reader pool.
func (r *DictPrefixReader) putBuilder(b *strings.Builder) {
	r.builders.Put(b)
}

// GetPrefix reconstructs the prefix string at the given position.
func (r *DictPrefixReader) GetPrefix(pos uint64) (string, error) {
	count := r.Count()
	if pos >= count {
		return "", ErrBoundsCheck
	}

	start, err := r.offsets.GetU64(pos)
	if err != nil {
		return "", fmt.Errorf("get start offset: %w", err)
	}

	end, err := r.offsets.GetU64(pos + 1)
	if err != nil {
		return "", fmt.Errorf("get end offset: %w", err)
	}

	numSegs := int(end - start)
	if numSegs == 0 {
		return "", nil
	}

	builder := r.getBuilder()
	builder.Grow(numSegs * avgSegmentBytes)

	for i := start; i < end; i++ {
		segID, err := r.segIDs.GetU32(i)
		if err != nil {
			r.putBuilder(builder)

			return "", fmt.Errorf("get segment ID at %d: %w", i, err)
		}
		if i > start {
			builder.WriteByte('/')
		}
		builder.WriteString(r.cache.Get(segID))
	}

	result := builder.String()
	r.putBuilder(builder)

	return result, nil
}

// UnsafeGetPrefix reconstructs the prefix without bounds checking.
func (r *DictPrefixReader) UnsafeGetPrefix(pos uint64) string {
	start := r.offsets.UnsafeGetU64(pos)
	end := r.offsets.UnsafeGetU64(pos + 1)

	numSegs := int(end - start)
	if numSegs == 0 {
		return ""
	}

	builder := r.getBuilder()
	builder.Grow(numSegs * avgSegmentBytes)

	for i := start; i < end; i++ {
		if i > start {
			builder.WriteByte('/')
		}
		segID := r.segIDs.UnsafeGetU32(i)
		builder.WriteString(r.cache.Get(segID))
	}

	result := builder.String()
	r.putBuilder(builder)

	return result
}

// Count returns the number of prefixes (N, not N+1).
func (r *DictPrefixReader) Count() uint64 {
	if r.offsets.Count() == 0 {
		return 0
	}

	return r.offsets.Count() - 1
}

// Close releases resources.
func (r *DictPrefixReader) Close() error {
	return errors.Join(r.dict.Close(), r.segIDs.Close(), r.offsets.Close())
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
