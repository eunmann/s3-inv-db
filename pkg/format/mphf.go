package format

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/relab/bbhash"
)

// DirPerm and FilePerm are the permission bits applied to every
// index directory and file the project creates. Owner-only access
// is the right default for a single-user cache + a single-user
// service. Exported so seeder/loader/server share one constant.
const (
	DirPerm  os.FileMode = 0o750
	FilePerm os.FileMode = 0o600
)

// MPHF provides read access to a prefix → preorder-position index.
//
// Thread Safety: safe for concurrent reads. Close once after all
// reads.
//
// The struct may host one of two forward-lookup backends:
//
//   - The classic MPHF backend uses BBHash + an interleaved
//     fingerprint+pos array (mph.bin + mph_fp_pos.u64).
//   - The FST backend uses a vellum FST (prefixes.fst).
//
// fstIdx is non-nil iff prefixes.fst was found on disk; in that
// case the MPHF arrays may be absent and Lookup routes through the
// FST. The reverse path (GetPrefix) always uses prefix-blob /
// prefix-dict regardless of which forward backend is active.
type MPHF struct {
	mph           *bbhash.BBHash2
	combined      *ArrayReader
	prefixBlob    *BlobReader
	dictPrefixes  *DictPrefixReader
	fstIdx        *FSTPrefixReader
	count         uint64
	usePrefixDict bool
}

// OpenMPHF opens a prefix-lookup index from the given directory.
// Detects which forward backend was written (FST if prefixes.fst is
// present, otherwise the classic MPHF). The reverse path picks up
// prefix_dict / prefix_blob via the same code in either case.
func OpenMPHF(outDir string) (*MPHF, error) {
	prefixBlob, dictPrefixes, usePrefixDict, err := openPrefixStorage(outDir)
	if err != nil {
		return nil, err
	}

	// FST backend: prefixes.fst exists. mph.bin / mph_fp_pos.u64 may
	// or may not exist; we don't read them.
	if FSTPresent(outDir) {
		fst, fstErr := OpenFSTPrefixReader(outDir)
		if fstErr != nil {
			closePrefixStorage(prefixBlob, dictPrefixes)

			return nil, fmt.Errorf("open fst prefix reader: %w", fstErr)
		}

		return &MPHF{
			prefixBlob:    prefixBlob,
			dictPrefixes:  dictPrefixes,
			fstIdx:        fst,
			count:         fst.Count(),
			usePrefixDict: usePrefixDict,
		}, nil
	}

	// Classic MPHF backend.
	mphPath := filepath.Join(outDir, MPHFile)
	combinedPath := filepath.Join(outDir, CombinedMPHFArrayFile)

	info, statErr := os.Stat(mphPath)
	if statErr != nil {
		closePrefixStorage(prefixBlob, dictPrefixes)

		return nil, fmt.Errorf("stat mph file: %w", statErr)
	}
	if info.Size() == 0 {
		closePrefixStorage(prefixBlob, dictPrefixes)

		return &MPHF{count: 0}, nil
	}

	mphData, readErr := os.ReadFile(mphPath)
	if readErr != nil {
		closePrefixStorage(prefixBlob, dictPrefixes)

		return nil, fmt.Errorf("read mph file: %w", readErr)
	}
	mph := &bbhash.BBHash2{}
	if err := mph.UnmarshalBinary(mphData); err != nil {
		closePrefixStorage(prefixBlob, dictPrefixes)

		return nil, fmt.Errorf("unmarshal MPHF: %w", err)
	}

	combined, openErr := OpenArrayWithHint(combinedPath, AccessHintRandom)
	if openErr != nil {
		closePrefixStorage(prefixBlob, dictPrefixes)

		return nil, fmt.Errorf("open combined fp+pos: %w", openErr)
	}

	return &MPHF{
		mph:           mph,
		combined:      combined,
		prefixBlob:    prefixBlob,
		dictPrefixes:  dictPrefixes,
		count:         combined.Count() / 2,
		usePrefixDict: usePrefixDict,
	}, nil
}

// openPrefixStorage opens prefix_dict.* if present, falling back to
// prefix_blob.bin + prefix_offsets.u64. Used by both forward
// backends — the reverse path is independent of which one's on disk.
//
//nolint:nonamedreturns // multi-result with clear names
func openPrefixStorage(outDir string) (blob *BlobReader, dict *DictPrefixReader, usePrefixDict bool, err error) {
	dictBlobPath := filepath.Join(outDir, PrefixDictBlobFile)
	if _, statErr := os.Stat(dictBlobPath); statErr == nil {
		dict, err = OpenDictPrefixReader(outDir)
		if err != nil {
			return nil, nil, false, fmt.Errorf("open dict prefixes: %w", err)
		}

		return nil, dict, true, nil
	}

	blobPath := filepath.Join(outDir, PrefixBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixOffsetsFile)
	if _, statErr := os.Stat(blobPath); statErr == nil {
		blob, err = OpenBlob(blobPath, offsetsPath)
		if err != nil {
			return nil, nil, false, fmt.Errorf("open prefix blob: %w", err)
		}
	}

	return blob, nil, false, nil
}

func closePrefixStorage(blob *BlobReader, dict *DictPrefixReader) {
	if blob != nil {
		_ = blob.Close()
	}
	if dict != nil {
		_ = dict.Close()
	}
}

// Close releases resources.
func (m *MPHF) Close() error {
	var combinedErr, blobErr, dictErr, fstErr error
	if m.combined != nil {
		combinedErr = m.combined.Close()
	}
	if m.prefixBlob != nil {
		blobErr = m.prefixBlob.Close()
	}
	if m.dictPrefixes != nil {
		dictErr = m.dictPrefixes.Close()
	}
	if m.fstIdx != nil {
		fstErr = m.fstIdx.Close()
	}

	return errors.Join(combinedErr, blobErr, dictErr, fstErr)
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (m *MPHF) Lookup(prefix string) (uint64, bool) {
	if m.fstIdx != nil {
		return m.fstIdx.Lookup(prefix)
	}
	if m.count == 0 || m.mph == nil {
		return 0, false
	}
	keyHash := hashString(prefix)
	hashVal := m.mph.Find(keyHash)
	if hashVal == 0 {
		return 0, false
	}
	hashPos := hashVal - 1
	if hashPos >= m.count {
		return 0, false
	}
	storedFP := m.combined.UnsafeGetU64(2 * hashPos)
	preorderPosVal := m.combined.UnsafeGetU64(2*hashPos + 1)
	if storedFP != computeFingerprint(prefix) {
		return 0, false
	}

	return preorderPosVal, true
}

// Prefix returns the prefix string at the given position.
// Requires the prefix blob to be loaded.
func (m *MPHF) Prefix(pos uint64) (string, error) {
	return m.GetPrefix(pos)
}

// GetPrefix returns the prefix string at the given position.
func (m *MPHF) GetPrefix(pos uint64) (string, error) {
	if m.usePrefixDict {
		s, err := m.dictPrefixes.GetPrefix(pos)
		if err != nil {
			return "", fmt.Errorf("get dict prefix at pos %d: %w", pos, err)
		}

		return s, nil
	}

	if m.prefixBlob == nil {
		return "", ErrPrefixBlobNotLoaded
	}
	s, err := m.prefixBlob.Get(pos)
	if err != nil {
		return "", fmt.Errorf("get prefix at pos %d: %w", pos, err)
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

	if m.usePrefixDict && m.dictPrefixes != nil {
		stored, err := m.dictPrefixes.GetPrefix(pos)
		if err != nil || stored != prefix {
			return 0, false
		}
	} else if m.prefixBlob != nil {
		stored, err := m.prefixBlob.Get(pos)
		if err != nil || stored != prefix {
			return 0, false
		}
	}

	return pos, true
}

// VerifyMPHF checks that every stored prefix round-trips: GetPrefix
// at every position returns a string that Lookups back to that position.
func VerifyMPHF(m *MPHF) error {
	if m.prefixBlob == nil && m.dictPrefixes == nil {
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

// hashString computes a uint64 hash for a string to use as MPHF key.
// Forwards to hashBytes via a zero-copy string-to-bytes conversion.
func hashString(s string) uint64 {
	return hashBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// hashBytes computes a uint64 hash for bytes to use as MPHF key.
func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)

	return h.Sum64()
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
