package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"unsafe"
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
	combined     *ArrayReader
	dictPrefixes *DictPrefixReader
	count        uint64
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

	dictPrefixes, err := OpenDictPrefixReader(outDir)
	if err != nil {
		combined.Close()

		return nil, fmt.Errorf("open dict prefixes: %w", err)
	}

	return &MPHF{
		mph:          mph,
		combined:     combined,
		dictPrefixes: dictPrefixes,
		count:        combined.Count() / 2,
		seed:         seed,
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
	var combinedErr, dictErr error
	if m.combined != nil {
		combinedErr = m.combined.Close()
	}
	if m.dictPrefixes != nil {
		dictErr = m.dictPrefixes.Close()
	}

	return errors.Join(combinedErr, dictErr)
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

func (m *MPHF) Prefix(pos uint64) (string, error) {
	return m.GetPrefix(pos)
}

func (m *MPHF) GetPrefix(pos uint64) (string, error) {
	if m.dictPrefixes == nil {
		return "", ErrNoPrefixStorage
	}
	s, err := m.dictPrefixes.GetPrefix(pos)
	if err != nil {
		return "", fmt.Errorf("get dict prefix at pos %d: %w", pos, err)
	}

	return s, nil
}

// LookupWithVerify returns the position and verifies the stored prefix
// matches exactly, eliminating fingerprint-collision false positives.
func (m *MPHF) LookupWithVerify(prefix string) (uint64, bool) {
	pos, ok := m.Lookup(prefix)
	if !ok {
		return 0, false
	}
	if m.dictPrefixes != nil {
		stored, err := m.dictPrefixes.GetPrefix(pos)
		if err != nil || stored != prefix {
			return 0, false
		}
	}

	return pos, true
}

// VerifyMPHF checks that every stored prefix round-trips: GetPrefix
// at every position returns a string that Lookups back to that position.
func VerifyMPHF(m *MPHF) error {
	if m.dictPrefixes == nil {
		return ErrNoPrefixStorage
	}

	for i := range m.count {
		prefix, err := m.GetPrefix(i)
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

// Count returns the number of entries in the MPHF.
func (m *MPHF) Count() uint64 {
	return m.count
}

const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// hashString computes the MPHF key hash of s. Forwards to hashBytes
// via a zero-copy string-to-bytes conversion.
func hashString(s string, seed uint64) uint64 {
	return hashBytes(unsafe.Slice(unsafe.StringData(s), len(s)), seed)
}

// hashBytes is 64-bit FNV-1a with the offset basis XORed with seed,
// so seed 0 is plain FNV-1a: the key hash of every index built before
// seeds existed.
func hashBytes(b []byte, seed uint64) uint64 {
	h := fnvOffset64 ^ seed
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}

	return h
}

// computeFingerprint computes a fingerprint for verification. Uses a
// different hash function to reduce collision probability. Forwards
// via zero-copy conversion to computeFingerprintBytes.
func computeFingerprint(s string) uint64 {
	return computeFingerprintBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// computeFingerprintBytes computes a fingerprint from bytes.
func computeFingerprintBytes(b []byte) uint64 {
	h := fnv.New64()
	h.Write(b)

	return h.Sum64()
}
