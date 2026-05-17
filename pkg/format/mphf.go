package format

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/relab/bbhash"
)

// indexFilePerm restricts MPHF and tier-stats files to owner read/write
// only; index directories are written by the seeder and read by the
// server process running as the same user.
const indexFilePerm = 0o600

// MPHF provides read access to the minimal perfect hash function.
//
// Thread Safety: MPHF is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type MPHF struct {
	mph *bbhash.BBHash2
	// combined holds the interleaved [fp, pos, fp, pos, ...] array
	// when present (new format). Lookup at hash position p reads
	// combined.UnsafeGetU64(2p) for fp and combined.UnsafeGetU64(2p+1)
	// for pos — adjacent words in the same array, typically the same
	// 64-byte cache line. Halves the cold-cache cost of a Lookup vs
	// the previous separate-array layout.
	combined *ArrayReader
	// fingerprints + preorderPos hold the legacy separate-array
	// layout. Populated only when an old index without combined is
	// opened. Mutually exclusive with `combined`.
	fingerprints *ArrayReader
	preorderPos  *ArrayReader // maps hash position -> preorder position
	prefixBlob   *BlobReader
	count        uint64
}

// OpenMPHF opens an MPHF from the given directory. New builds emit a
// single combined fp+pos file; older builds had separate files. We
// handle either layout transparently.
func OpenMPHF(outDir string) (*MPHF, error) {
	mphPath := filepath.Join(outDir, "mph.bin")
	combinedPath := filepath.Join(outDir, CombinedMPHFArrayFile)
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

	// Prefer the combined fp+pos file (new format). Fall back to
	// separate fp + pos files if the combined one isn't present.
	var combined, fingerprints, preorderPos *ArrayReader
	if _, err := os.Stat(combinedPath); err == nil {
		combined, err = OpenArrayWithHint(combinedPath, AccessHintRandom)
		if err != nil {
			return nil, fmt.Errorf("open combined fp+pos: %w", err)
		}
	} else {
		fingerprints, err = OpenArrayWithHint(fpPath, AccessHintRandom)
		if err != nil {
			return nil, fmt.Errorf("open fingerprints: %w", err)
		}
		preorderPos, err = OpenArrayWithHint(posPath, AccessHintRandom)
		if err != nil {
			fingerprints.Close()

			return nil, fmt.Errorf("open preorder positions: %w", err)
		}
	}

	// Load prefix blob (raw encoding; segmented removed).
	var prefixBlob *BlobReader
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")

	if _, err := os.Stat(blobPath); err == nil {
		prefixBlob, err = OpenBlob(blobPath, offsetsPath)
		if err != nil {
			if fingerprints != nil {
				fingerprints.Close()
			}
			if preorderPos != nil {
				preorderPos.Close()
			}

			return nil, fmt.Errorf("open prefix blob: %w", err)
		}
	}

	return &MPHF{
		mph:          mph,
		combined:     combined,
		fingerprints: fingerprints,
		preorderPos:  preorderPos,
		prefixBlob:   prefixBlob,
		count:        mphCount(combined, fingerprints),
	}, nil
}

// mphCount returns the prefix count from whichever array layout the
// MPHF is using. The combined array stores 2 entries per prefix; the
// separate fingerprints array stores 1.
func mphCount(combined, fingerprints *ArrayReader) uint64 {
	if combined != nil {
		return combined.Count() / 2
	}
	if fingerprints != nil {
		return fingerprints.Count()
	}

	return 0
}

// Close releases resources.
func (m *MPHF) Close() error {
	var firstErr error

	if m.combined != nil {
		if err := m.combined.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

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
func (m *MPHF) Lookup(prefix string) (uint64, bool) {
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

	// Verify with fingerprint, then read preorder position. When the
	// combined interleaved layout is in use both reads land in the
	// same cache line (slot 2p = fp, slot 2p+1 = pos).
	var storedFP, preorderPosVal uint64
	if m.combined != nil {
		storedFP = m.combined.UnsafeGetU64(2 * hashPos)
		preorderPosVal = m.combined.UnsafeGetU64(2*hashPos + 1)
	} else {
		storedFP = m.fingerprints.UnsafeGetU64(hashPos)
		preorderPosVal = m.preorderPos.UnsafeGetU64(hashPos)
	}
	if storedFP != computeFingerprint(prefix) {
		return 0, false
	}

	return preorderPosVal, true
}

// LookupWithVerify returns the position and verifies against the
// stored prefix in the blob (more certain than fingerprint-only).
func (m *MPHF) LookupWithVerify(prefix string) (uint64, bool) {
	pos, ok := m.Lookup(prefix)
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

// Prefix returns the prefix string at the given position.
// Requires the prefix blob to be loaded.
func (m *MPHF) Prefix(pos uint64) (string, error) {
	if m.prefixBlob == nil {
		return "", ErrPrefixBlobNotLoaded
	}
	s, err := m.prefixBlob.Get(pos)
	if err != nil {
		return "", fmt.Errorf("get prefix at pos %d: %w", pos, err)
	}

	return s, nil
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

// hashBytes computes a uint64 hash for bytes to use as MPHF key.
// This avoids the string allocation in hashString.
func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)

	return h.Sum64()
}

// computeFingerprint computes a fingerprint for verification.
// Uses a different hash function to reduce collision probability.
func computeFingerprint(s string) uint64 {
	h := fnv.New64()
	h.Write([]byte(s))

	return h.Sum64()
}

// computeFingerprintBytes computes a fingerprint from bytes.
// This avoids the string allocation in computeFingerprint.
func computeFingerprintBytes(b []byte) uint64 {
	h := fnv.New64()
	h.Write(b)

	return h.Sum64()
}

// VerifyMPHF checks that all prefixes can be looked up correctly.
func VerifyMPHF(m *MPHF) error {
	if m.prefixBlob == nil {
		return ErrNoPrefixStorage
	}

	for i := range m.count {
		prefix, err := m.Prefix(i)
		if err != nil {
			return fmt.Errorf("get prefix %d: %w", i, err)
		}

		pos, ok := m.Lookup(prefix)
		if !ok {
			return fmt.Errorf("%w for prefix %q at pos %d", ErrMPHFLookupFailed, prefix, i)
		}
		if pos != i {
			return fmt.Errorf("%w for %q: got %d, want %d", ErrLookupWrongPos, prefix, pos, i)
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
		return fmt.Errorf("create blob writer: %w", err)
	}

	for i, p := range prefixes {
		if err := writer.WriteString(p); err != nil {
			writer.Close()

			return fmt.Errorf("write prefix %d: %w", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close blob writer: %w", err)
	}

	return nil
}
