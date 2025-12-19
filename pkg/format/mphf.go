package format

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/relab/bbhash"
)

// MPHFBuilder builds a minimal perfect hash function for prefix strings.
type MPHFBuilder struct {
	prefixes    []string
	preorderPos []uint64 // original preorder positions
}

// NewMPHFBuilder creates a new MPHF builder.
func NewMPHFBuilder() *MPHFBuilder {
	return &MPHFBuilder{}
}

// Add adds a prefix at the given preorder position.
func (b *MPHFBuilder) Add(prefix string, pos uint64) {
	b.prefixes = append(b.prefixes, prefix)
	b.preorderPos = append(b.preorderPos, pos)
}

// Build constructs the MPHF and writes it to the output directory.
// It also writes fingerprints for verification.
func (b *MPHFBuilder) Build(outDir string) error {
	if len(b.prefixes) == 0 {
		return b.writeEmpty(outDir)
	}

	// Hash all prefixes to uint64 for bbhash
	keys := make([]uint64, len(b.prefixes))
	for i, p := range b.prefixes {
		keys[i] = hashString(p)
	}

	// Build MPHF with gamma=2.0 (good space/time tradeoff)
	mph, err := bbhash.New(keys, bbhash.Gamma(2.0))
	if err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}

	// Write MPHF
	mphPath := filepath.Join(outDir, "mph.bin")
	mphFile, err := os.Create(mphPath)
	if err != nil {
		return fmt.Errorf("create mph file: %w", err)
	}

	data, err := mph.MarshalBinary()
	if err != nil {
		mphFile.Close()
		os.Remove(mphPath)
		return fmt.Errorf("marshal MPHF: %w", err)
	}
	if _, err := mphFile.Write(data); err != nil {
		mphFile.Close()
		os.Remove(mphPath)
		return fmt.Errorf("write MPHF: %w", err)
	}
	mphFile.Close()

	// BBHash returns 1-indexed values, so we create arrays indexed 0..N-1
	// where element[mphfPos] = data for prefix that maps to mphfPos
	orderedPrefixes := make([]string, len(b.prefixes))
	fingerprints := make([]uint64, len(b.prefixes))
	preorderPositions := make([]uint64, len(b.prefixes))

	for i, prefix := range b.prefixes {
		keyHash := hashString(prefix)
		hashVal := mph.Find(keyHash)
		if hashVal == 0 {
			return fmt.Errorf("MPHF lookup failed for %q", prefix)
		}
		hashPos := hashVal - 1 // Convert to 0-indexed
		fingerprints[hashPos] = computeFingerprint(prefix)
		orderedPrefixes[hashPos] = prefix
		preorderPositions[hashPos] = b.preorderPos[i]
	}

	// Write fingerprints
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	fpWriter, err := NewArrayWriter(fpPath, 8)
	if err != nil {
		return fmt.Errorf("create fingerprint writer: %w", err)
	}

	for _, fp := range fingerprints {
		if err := fpWriter.WriteU64(fp); err != nil {
			fpWriter.Close()
			return fmt.Errorf("write fingerprint: %w", err)
		}
	}

	if err := fpWriter.Close(); err != nil {
		return fmt.Errorf("close fingerprint writer: %w", err)
	}

	// Write preorder positions
	posPath := filepath.Join(outDir, "mph_pos.u64")
	posWriter, err := NewArrayWriter(posPath, 8)
	if err != nil {
		return fmt.Errorf("create position writer: %w", err)
	}

	for _, p := range preorderPositions {
		if err := posWriter.WriteU64(p); err != nil {
			posWriter.Close()
			return fmt.Errorf("write preorder position: %w", err)
		}
	}

	if err := posWriter.Close(); err != nil {
		return fmt.Errorf("close position writer: %w", err)
	}

	// Write prefix blob in preorder order (for GetPrefix by preorder position)
	if err := WritePrefixBlob(outDir, b.prefixes); err != nil {
		return fmt.Errorf("write prefix blob: %w", err)
	}

	return nil
}

func (b *MPHFBuilder) writeEmpty(outDir string) error {
	// Create empty mph file
	mphPath := filepath.Join(outDir, "mph.bin")
	if err := os.WriteFile(mphPath, nil, 0644); err != nil {
		return fmt.Errorf("write empty mph: %w", err)
	}

	// Create empty fingerprint array
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	fpWriter, err := NewArrayWriter(fpPath, 8)
	if err != nil {
		return err
	}
	if err := fpWriter.Close(); err != nil {
		return err
	}

	// Create empty position array
	posPath := filepath.Join(outDir, "mph_pos.u64")
	posWriter, err := NewArrayWriter(posPath, 8)
	if err != nil {
		return err
	}
	if err := posWriter.Close(); err != nil {
		return err
	}

	// Create empty prefix blob files
	if err := WritePrefixBlob(outDir, nil); err != nil {
		return fmt.Errorf("write empty prefix blob: %w", err)
	}

	return nil
}

// Count returns the number of prefixes added.
func (b *MPHFBuilder) Count() int {
	return len(b.prefixes)
}

// MPHF provides read access to the minimal perfect hash function.
type MPHF struct {
	mph          *bbhash.BBHash2
	fingerprints *ArrayReader
	preorderPos  *ArrayReader // maps hash position -> preorder position
	prefixBlob   *BlobReader
	count        uint64
}

// OpenMPHF opens an MPHF from the given directory.
func OpenMPHF(outDir string) (*MPHF, error) {
	mphPath := filepath.Join(outDir, "mph.bin")
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	posPath := filepath.Join(outDir, "mph_pos.u64")

	// Check if empty
	info, err := os.Stat(mphPath)
	if err != nil {
		return nil, fmt.Errorf("stat mph file: %w", err)
	}

	if info.Size() == 0 {
		// Empty MPHF
		return &MPHF{count: 0}, nil
	}

	// Load MPHF
	mphData, err := os.ReadFile(mphPath)
	if err != nil {
		return nil, fmt.Errorf("read mph file: %w", err)
	}

	mph := &bbhash.BBHash2{}
	if err := mph.UnmarshalBinary(mphData); err != nil {
		return nil, fmt.Errorf("unmarshal MPHF: %w", err)
	}

	// Load fingerprints
	fingerprints, err := OpenArray(fpPath)
	if err != nil {
		return nil, fmt.Errorf("open fingerprints: %w", err)
	}

	// Load preorder positions
	preorderPos, err := OpenArray(posPath)
	if err != nil {
		fingerprints.Close()
		return nil, fmt.Errorf("open preorder positions: %w", err)
	}

	// Optionally load prefix blob (for reverse lookup)
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")

	var prefixBlob *BlobReader
	if _, err := os.Stat(blobPath); err == nil {
		prefixBlob, err = OpenBlob(blobPath, offsetsPath)
		if err != nil {
			fingerprints.Close()
			preorderPos.Close()
			return nil, fmt.Errorf("open prefix blob: %w", err)
		}
	}

	return &MPHF{
		mph:          mph,
		fingerprints: fingerprints,
		preorderPos:  preorderPos,
		prefixBlob:   prefixBlob,
		count:        fingerprints.Count(),
	}, nil
}

// Close releases resources.
func (m *MPHF) Close() error {
	var firstErr error

	if m.fingerprints != nil {
		if err := m.fingerprints.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if m.preorderPos != nil {
		if err := m.preorderPos.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if m.prefixBlob != nil {
		if err := m.prefixBlob.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (m *MPHF) Lookup(prefix string) (pos uint64, ok bool) {
	if m.count == 0 || m.mph == nil {
		return 0, false
	}

	keyHash := hashString(prefix)
	hashVal := m.mph.Find(keyHash)
	if hashVal == 0 {
		return 0, false
	}

	hashPos := hashVal - 1 // Convert to 0-indexed

	if hashPos >= m.count {
		return 0, false
	}

	// Verify with fingerprint
	storedFP := m.fingerprints.UnsafeGetU64(hashPos)
	computedFP := computeFingerprint(prefix)

	if storedFP != computedFP {
		return 0, false
	}

	// Return the preorder position, not the hash position
	return m.preorderPos.UnsafeGetU64(hashPos), true
}

// LookupWithVerify returns the position and optionally verifies against
// the prefix blob (slower but more certain).
func (m *MPHF) LookupWithVerify(prefix string) (pos uint64, ok bool) {
	pos, ok = m.Lookup(prefix)
	if !ok {
		return 0, false
	}

	if m.prefixBlob != nil {
		storedPrefix, err := m.prefixBlob.Get(pos)
		if err != nil || storedPrefix != prefix {
			return 0, false
		}
	}

	return pos, true
}

// GetPrefix returns the prefix string at the given position.
// Requires prefix blob to be loaded.
func (m *MPHF) GetPrefix(pos uint64) (string, error) {
	if m.prefixBlob == nil {
		return "", fmt.Errorf("prefix blob not loaded")
	}
	return m.prefixBlob.Get(pos)
}

// Count returns the number of entries in the MPHF.
func (m *MPHF) Count() uint64 {
	return m.count
}

// hashString computes a uint64 hash for a string to use as MPHF key.
func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// computeFingerprint computes a fingerprint for verification.
// Uses a different hash function to reduce collision probability.
func computeFingerprint(s string) uint64 {
	h := fnv.New64()
	h.Write([]byte(s))
	return h.Sum64()
}

// VerifyMPHF checks that all prefixes in the blob can be looked up correctly.
func VerifyMPHF(m *MPHF) error {
	if m.prefixBlob == nil {
		return fmt.Errorf("prefix blob not loaded")
	}

	for i := uint64(0); i < m.count; i++ {
		prefix, err := m.prefixBlob.Get(i)
		if err != nil {
			return fmt.Errorf("get prefix %d: %w", i, err)
		}

		pos, ok := m.Lookup(prefix)
		if !ok {
			return fmt.Errorf("lookup failed for prefix %q at pos %d", prefix, i)
		}
		if pos != i {
			return fmt.Errorf("lookup returned wrong pos for %q: got %d, want %d", prefix, pos, i)
		}
	}

	return nil
}

// WritePrefixBlob writes prefix strings from a slice.
func WritePrefixBlob(outDir string, prefixes []string) error {
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")

	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return err
	}

	for _, p := range prefixes {
		if err := writer.WriteString(p); err != nil {
			writer.Close()
			return err
		}
	}

	return writer.Close()
}
