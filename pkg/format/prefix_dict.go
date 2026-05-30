package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
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
//
// During the build segment IDs are appended to a wide-stride (4 B
// per ID) scratch file. At Close, the writer chooses the smallest
// byte width that holds the observed max segment ID and repacks the
// scratch file into the final tight-width ids file. The Header.Width
// of the final file is the chosen byte width; the reader honours it.
type DictPrefixWriter struct {
	interner      *PrefixSegmentInterner
	segIDsWriter  *ArrayWriter
	offsetsWriter *ArrayWriter
	finalIDsPath  string
	scratchPath   string
	currentOffset uint64
}

// NewDictPrefixWriter creates a writer for dictionary-encoded prefix
// storage.
func NewDictPrefixWriter(outDir string) (*DictPrefixWriter, error) {
	interner, err := NewPrefixSegmentInterner(outDir)
	if err != nil {
		return nil, fmt.Errorf("create prefix segment interner: %w", err)
	}

	finalIDsPath := filepath.Join(outDir, PrefixDictIDsFile)
	scratchPath := finalIDsPath + ".wide"
	segIDsWriter, err := NewArrayWriter(scratchPath, 4)
	if err != nil {
		interner.Close()

		return nil, fmt.Errorf("create dict IDs scratch writer: %w", err)
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
		finalIDsPath:  finalIDsPath,
		scratchPath:   scratchPath,
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

// dictIDsTailPad is the number of zero bytes appended to the final
// ids file so a reader can do a single 4-byte LE load + mask at any
// valid element offset (including the last) without faulting past
// the mmap.
const dictIDsTailPad = 4

// Close finalizes all output files, writing the sentinel offset.
// After the scratch wide ids file is closed, repacks it into the
// final ids file at the smallest byte width that holds the observed
// max segment ID.
func (w *DictPrefixWriter) Close() error {
	if err := w.offsetsWriter.WriteU64(w.currentOffset); err != nil {
		return fmt.Errorf("write sentinel offset: %w", err)
	}

	maxSegmentID := uint32(0)
	if w.interner.Count() > 0 {
		maxSegmentID = w.interner.Count() - 1
	}

	var errs []error
	if err := w.interner.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close interner: %w", err))
	}
	if err := w.segIDsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close scratch IDs: %w", err))
	}
	if err := w.offsetsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close offsets: %w", err))
	}
	if len(errs) > 0 {
		_ = os.Remove(w.scratchPath)

		return errors.Join(errs...)
	}

	width := byteWidthOf(uint64(maxSegmentID))
	if err := repackDictIDs(w.scratchPath, w.finalIDsPath, width); err != nil {
		_ = os.Remove(w.scratchPath)

		return fmt.Errorf("repack dict IDs to width %d: %w", width, err)
	}
	if err := os.Remove(w.scratchPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove scratch IDs: %w", err)
	}

	// Adaptive width on the per-prefix offsets file too: max value is
	// the trailing sentinel = w.currentOffset (total segment-ID refs).
	offsetsPath := filepath.Join(filepath.Dir(w.scratchPath), PrefixDictOffsetsPerPrefixFile)
	offWidth := byteWidthOf(w.currentOffset)
	if err := RepackArrayWidthU64(offsetsPath, offsetsPath, offWidth); err != nil {
		return fmt.Errorf("repack prefix_off to width %d: %w", offWidth, err)
	}

	return nil
}

// repackDictIDs reads the wide-stride scratch file (Header.Width=4)
// and writes the final ids file at the chosen byte width. Streams
// the data so the in-memory cost stays at one bufio chunk.
func repackDictIDs(srcPath, dstPath string, width uint8) error {
	src, err := OpenArray(srcPath)
	if err != nil {
		return fmt.Errorf("open scratch: %w", err)
	}
	defer src.Close()
	count := src.Count()

	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create final: %w", err)
	}
	if _, err := dst.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   count,
		Width:   uint32(width),
	})); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)

		return fmt.Errorf("write header: %w", err)
	}

	bw := bufio.NewWriterSize(dst, repackBufferSize)
	var buf [4]byte
	for i := range count {
		binary.LittleEndian.PutUint32(buf[:], src.UnsafeGetU32(i))
		if _, err := bw.Write(buf[:width]); err != nil {
			_ = dst.Close()
			_ = os.Remove(dstPath)

			return fmt.Errorf("write id: %w", err)
		}
	}
	// Tail pad so the reader's 4-byte masked load is always in-bounds.
	zero := [dictIDsTailPad]byte{}
	if _, err := bw.Write(zero[:]); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)

		return fmt.Errorf("write tail pad: %w", err)
	}
	if err := bw.Flush(); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)

		return fmt.Errorf("flush: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()

		return fmt.Errorf("sync: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
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
//
// The segment-ID array width is per-index: 1..4 bytes per ID,
// determined at Close from the observed max segment ID. IdsRaw is
// the raw byte slice of the segIDs file body (after Header), and
// idsWidth is the byte width per ID. The file is padded with 4
// trailing zero bytes so a single LE-load + mask at any valid index
// is always safe.
type DictPrefixReader struct {
	dict     *PrefixDictionary
	cache    *PreloadedPrefixCache
	segIDs   *ArrayReader
	offsets  *ArrayReader
	idsRaw   []byte
	builders sync.Pool
	idsWidth uint64
	idsMask  uint32
	idsCount uint64
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
	w := uint64(segIDs.Width())
	if w < 1 || w > 4 {
		dict.Close()
		segIDs.Close()

		return nil, fmt.Errorf("dict ids: %w: width=%d", ErrWidthMismatch, w)
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
		idsRaw:   segIDs.mmap.Data()[HeaderSize:],
		idsWidth: w,
		idsMask:  uint32(widthMask[w]),
		idsCount: segIDs.Count(),
		builders: sync.Pool{New: func() any { return &strings.Builder{} }},
	}, nil
}

// readSegID reads the segment ID at index i from the variable-width
// segIDs file. The file is tail-padded so a 4-byte LE load is always
// safe; the mask cuts the load to the actual byte width.
func (r *DictPrefixReader) readSegID(i uint64) uint32 {
	off := i * r.idsWidth
	v := uint32(r.idsRaw[off]) |
		uint32(r.idsRaw[off+1])<<8 |
		uint32(r.idsRaw[off+2])<<16 |
		uint32(r.idsRaw[off+3])<<24

	return v & r.idsMask
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
		if i >= r.idsCount {
			r.putBuilder(builder)

			return "", fmt.Errorf("get segment ID at %d: %w", i, ErrBoundsCheck)
		}
		segID := r.readSegID(i)
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
		builder.WriteString(r.cache.Get(r.readSegID(i)))
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
