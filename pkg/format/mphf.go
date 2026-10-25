package format

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// DirPerm and FilePerm are the permission bits applied to every
// index directory and file the project creates. Owner-only access
// is the right default for a single-user cache + a single-user
// service. Exported so seeder/loader/server share one constant.
const (
	DirPerm  os.FileMode = 0o750
	FilePerm os.FileMode = 0o600
)

// MPHF provides read access to the minimal perfect hash function.
//
// Thread Safety: MPHF is safe for concurrent read access from
// multiple goroutines. Close once, after all reads.
type MPHF struct {
	mph *bbhash
	// combined holds the interleaved [fp, pos, fp, pos, ...] array.
	// Lookup at hash position p reads combined.UnsafeGetU64(2p) for
	// fp and combined.UnsafeGetU64(2p+1) for pos — adjacent words
	// in the same cache line.
	combined *ArrayReader
	count    uint64
	// seed is XORed into the key hash's offset basis; see
	// writeCombinedMmap for where it is stored.
	seed uint64
}

// OpenMPHF opens an MPHF from the given directory.
func OpenMPHF(outDir string) (*MPHF, error) {
	mphPath := filepath.Join(outDir, MPHFile)
	combinedPath := filepath.Join(outDir, CombinedMPHFArrayFile)

	info, err := os.Stat(mphPath)
	if err != nil {
		return nil, fmt.Errorf("stat mph file: %w", err)
	}
	if info.Size() == 0 {
		return &MPHF{count: 0}, nil
	}

	mph, err := readBBHash(mphPath)
	if err != nil {
		return nil, err
	}

	combined, err := OpenArrayWithHint(combinedPath, AccessHintRandom)
	if err != nil {
		return nil, fmt.Errorf("open combined fp+pos: %w", err)
	}
	seed, err := mphfSeedOf(combined)
	if err != nil {
		combined.Close()

		return nil, err
	}

	return &MPHF{
		mph:      mph,
		combined: combined,
		count:    combined.Count() / 2,
		seed:     seed,
	}, nil
}

// mphfSeedOf returns the key-hash seed stored after the fp/pos slots.
func mphfSeedOf(combined *ArrayReader) (uint64, error) {
	end := combined.header.Count * uint64(combined.header.Width)
	if uint64(len(combined.data)) < end+mphfSeedBytes {
		return 0, fmt.Errorf("read MPHF seed: %w", ErrFileTooSmall)
	}

	return binary.LittleEndian.Uint64(combined.data[end:]), nil
}

// Close releases resources.
func (m *MPHF) Close() error {
	if m.combined == nil {
		return nil
	}

	return m.combined.Close()
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (m *MPHF) Lookup(prefix string) (uint64, bool) {
	if m.count == 0 || m.mph == nil {
		return 0, false
	}
	keyHash := hashString(prefix, m.seed)
	hashPos, ok := m.mph.find(keyHash)
	if !ok || hashPos >= m.count {
		return 0, false
	}
	storedFP := m.combined.UnsafeGetU64(2 * hashPos)
	preorderPosVal := m.combined.UnsafeGetU64(2*hashPos + 1)
	if storedFP != computeFingerprint(prefix) {
		return 0, false
	}

	return preorderPosVal, true
}

// Count returns the number of entries in the MPHF.
func (m *MPHF) Count() uint64 {
	return m.count
}

// hashString is the MPHF key hash of s: wyhash with the index's key
// seed (0 unless a collision forced a retry; see mphfSeed).
func hashString(s string, seed uint64) uint64 {
	return wyhash(s, seed)
}

// fingerprintSeed seeds the slot fingerprint. It is fixed, so a key
// seed retry never changes the fingerprint, and differs from every key
// seed (a test pins this), so the fingerprint is independent of the
// key hash that chose the slot.
const fingerprintSeed = 0x6a09e667f3bcc909 // the first 64 bits of frac(sqrt(2))

// computeFingerprint is the slot fingerprint of s.
func computeFingerprint(s string) uint64 {
	return wyhash(s, fingerprintSeed)
}
