package format

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strings"
)

// Segment compression file names.
const (
	SegmentsBlobFile     = "segments.bin"
	SegmentsOffsetsFile  = "segments.off.u64"
	PrefixSegIDsFile     = "prefix_seg_ids.u32"
	PrefixSegOffsetsFile = "prefix_seg_off.u64"
)

// SegmentInterner interns unique path segments during index building.
// It assigns a sequential uint32 ID to each unique segment string.
//
// Segments are split from prefixes by "/" delimiter. For example:
//
//	"data/2024/01/" → ["data", "2024", "01", ""]
//
// The empty string after a trailing slash is significant - it distinguishes
// prefixes from keys (prefixes always end with "/").
type SegmentInterner struct {
	segmentMap map[uint64]uint32 // FNV hash → ID
	segments   []string          // ID → segment (for collision detection)
	blobWriter *BlobWriter
	nextID     uint32
}

// NewSegmentInterner creates a new segment interner that writes to outDir.
func NewSegmentInterner(outDir string) (*SegmentInterner, error) {
	blobPath := filepath.Join(outDir, SegmentsBlobFile)
	offsetsPath := filepath.Join(outDir, SegmentsOffsetsFile)

	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("create segment blob writer: %w", err)
	}

	return &SegmentInterner{
		segmentMap: make(map[uint64]uint32),
		segments:   make([]string, 0, 1024),
		blobWriter: writer,
		nextID:     0,
	}, nil
}

// Intern returns the ID for a segment, creating a new ID if this is the first occurrence.
// The segment is written to the blob file on first occurrence.
func (si *SegmentInterner) Intern(segment string) (uint32, error) {
	h := hashSegment(segment)

	// Check if we've seen this hash before
	if id, ok := si.segmentMap[h]; ok {
		// Verify it's actually the same segment (collision check)
		if si.segments[id] != segment {
			return 0, fmt.Errorf("hash collision between %q and %q", si.segments[id], segment)
		}
		return id, nil
	}

	// New segment - assign next ID
	id := si.nextID
	si.nextID++

	// Write to blob
	if err := si.blobWriter.WriteString(segment); err != nil {
		return 0, fmt.Errorf("write segment: %w", err)
	}

	// Record in maps
	si.segmentMap[h] = id
	si.segments = append(si.segments, segment)

	return id, nil
}

// Count returns the number of unique segments interned.
func (si *SegmentInterner) Count() uint32 {
	return si.nextID
}

// Close finalizes the segment blob file.
func (si *SegmentInterner) Close() error {
	return si.blobWriter.Close()
}

// hashSegment computes an FNV hash for segment deduplication.
func hashSegment(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// SegmentDictionary provides runtime lookup of segment strings by ID.
type SegmentDictionary struct {
	blob *BlobReader
}

// OpenSegmentDictionary opens a segment dictionary from disk.
func OpenSegmentDictionary(outDir string) (*SegmentDictionary, error) {
	blobPath := filepath.Join(outDir, SegmentsBlobFile)
	offsetsPath := filepath.Join(outDir, SegmentsOffsetsFile)

	blob, err := OpenBlob(blobPath, offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("open segment blob: %w", err)
	}

	return &SegmentDictionary{blob: blob}, nil
}

// GetSegment returns the segment string for the given ID.
func (sd *SegmentDictionary) GetSegment(id uint32) (string, error) {
	return sd.blob.Get(uint64(id))
}

// UnsafeGetSegment returns the segment without bounds checking.
func (sd *SegmentDictionary) UnsafeGetSegment(id uint32) string {
	return sd.blob.UnsafeGet(uint64(id))
}

// Count returns the number of segments in the dictionary.
func (sd *SegmentDictionary) Count() uint64 {
	return sd.blob.Count()
}

// Close releases resources.
func (sd *SegmentDictionary) Close() error {
	return sd.blob.Close()
}

// SegmentedPrefixWriter writes prefixes as sequences of segment IDs.
type SegmentedPrefixWriter struct {
	interner      *SegmentInterner
	segIDsWriter  *ArrayWriter // prefix_seg_ids.u32
	offsetsWriter *ArrayWriter // prefix_seg_off.u64
	currentOffset uint64
}

// NewSegmentedPrefixWriter creates a writer for segmented prefix encoding.
func NewSegmentedPrefixWriter(outDir string) (*SegmentedPrefixWriter, error) {
	interner, err := NewSegmentInterner(outDir)
	if err != nil {
		return nil, fmt.Errorf("create segment interner: %w", err)
	}

	segIDsPath := filepath.Join(outDir, PrefixSegIDsFile)
	segIDsWriter, err := NewArrayWriter(segIDsPath, 4)
	if err != nil {
		interner.Close()
		return nil, fmt.Errorf("create seg IDs writer: %w", err)
	}

	offsetsPath := filepath.Join(outDir, PrefixSegOffsetsFile)
	offsetsWriter, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		interner.Close()
		segIDsWriter.Close()
		return nil, fmt.Errorf("create seg offsets writer: %w", err)
	}

	return &SegmentedPrefixWriter{
		interner:      interner,
		segIDsWriter:  segIDsWriter,
		offsetsWriter: offsetsWriter,
		currentOffset: 0,
	}, nil
}

// WritePrefix splits a prefix into segments, interns each segment,
// and writes the segment IDs to the output files.
func (w *SegmentedPrefixWriter) WritePrefix(prefix string) error {
	// Write offset for this prefix
	if err := w.offsetsWriter.WriteU64(w.currentOffset); err != nil {
		return fmt.Errorf("write prefix offset: %w", err)
	}

	// Split prefix into segments
	segments := SplitPrefix(prefix)

	// Intern and write each segment ID
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

// Close finalizes all output files, writing sentinel offset.
func (w *SegmentedPrefixWriter) Close() error {
	// Write sentinel offset (points past end)
	if err := w.offsetsWriter.WriteU64(w.currentOffset); err != nil {
		return fmt.Errorf("write sentinel offset: %w", err)
	}

	var errs []error
	if err := w.interner.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close interner: %w", err))
	}
	if err := w.segIDsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close seg IDs: %w", err))
	}
	if err := w.offsetsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close offsets: %w", err))
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// SegmentCount returns the number of unique segments interned so far.
func (w *SegmentedPrefixWriter) SegmentCount() uint32 {
	return w.interner.Count()
}

// PrefixCount returns the number of prefixes written so far.
func (w *SegmentedPrefixWriter) PrefixCount() uint64 {
	return w.offsetsWriter.Count()
}

// SegmentedPrefixReader reads prefixes from segmented encoding.
type SegmentedPrefixReader struct {
	dict    *SegmentDictionary
	segIDs  *ArrayReader // prefix_seg_ids.u32
	offsets *ArrayReader // prefix_seg_off.u64
}

// OpenSegmentedPrefixReader opens a segmented prefix reader from disk.
func OpenSegmentedPrefixReader(outDir string) (*SegmentedPrefixReader, error) {
	dict, err := OpenSegmentDictionary(outDir)
	if err != nil {
		return nil, fmt.Errorf("open segment dictionary: %w", err)
	}

	segIDsPath := filepath.Join(outDir, PrefixSegIDsFile)
	segIDs, err := OpenArray(segIDsPath)
	if err != nil {
		dict.Close()
		return nil, fmt.Errorf("open seg IDs: %w", err)
	}

	offsetsPath := filepath.Join(outDir, PrefixSegOffsetsFile)
	offsets, err := OpenArray(offsetsPath)
	if err != nil {
		dict.Close()
		segIDs.Close()
		return nil, fmt.Errorf("open offsets: %w", err)
	}

	return &SegmentedPrefixReader{
		dict:    dict,
		segIDs:  segIDs,
		offsets: offsets,
	}, nil
}

// GetPrefix reconstructs the prefix string at the given position.
func (r *SegmentedPrefixReader) GetPrefix(pos uint64) (string, error) {
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

	// Reconstruct prefix from segments
	var builder strings.Builder
	for i := start; i < end; i++ {
		segID, err := r.segIDs.GetU32(i)
		if err != nil {
			return "", fmt.Errorf("get segment ID at %d: %w", i, err)
		}
		seg, err := r.dict.GetSegment(segID)
		if err != nil {
			return "", fmt.Errorf("get segment %d: %w", segID, err)
		}
		if i > start {
			builder.WriteByte('/')
		}
		builder.WriteString(seg)
	}

	return builder.String(), nil
}

// UnsafeGetPrefix reconstructs the prefix without bounds checking.
func (r *SegmentedPrefixReader) UnsafeGetPrefix(pos uint64) string {
	start := r.offsets.UnsafeGetU64(pos)
	end := r.offsets.UnsafeGetU64(pos + 1)

	// Reconstruct prefix from segments
	var builder strings.Builder
	for i := start; i < end; i++ {
		segID := r.segIDs.UnsafeGetU32(i)
		seg := r.dict.UnsafeGetSegment(segID)
		if i > start {
			builder.WriteByte('/')
		}
		builder.WriteString(seg)
	}

	return builder.String()
}

// Count returns the number of prefixes (N, not N+1).
func (r *SegmentedPrefixReader) Count() uint64 {
	if r.offsets.Count() == 0 {
		return 0
	}
	return r.offsets.Count() - 1
}

// Close releases resources.
func (r *SegmentedPrefixReader) Close() error {
	var firstErr error

	if err := r.dict.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := r.segIDs.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := r.offsets.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// SplitPrefix splits a prefix into path segments by "/".
// Trailing empty strings are preserved to distinguish prefixes from keys.
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
