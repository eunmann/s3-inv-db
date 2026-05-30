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
// Thread Safety: safe for concurrent reads. Close once after all reads.
//
// The struct hosts two on-disk backends:
//
//   - The "classic" MPHF backend uses BBHash + an interleaved
//     fingerprint+pos array (`mph.bin`, `mph_fp_pos.u64`) for the
//     forward (prefix → pos) lookup, plus either a raw prefix blob
//     or a segment dictionary for the reverse (pos → prefix) walk.
//   - The "FST" backend uses a single vellum FST for the forward
//     lookup plus a sparse anchor table for the reverse walk.
//
// fstIdx is non-nil iff the FST backend is in use. When it is, the
// BBHash/dict fields are nil and all calls route through fstIdx.
type MPHF struct {
	mph          *bbhash.BBHash2
	combined     *ArrayReader
	prefixBlob   *BlobReader
	dictPrefixes *DictPrefixReader
	fstIdx       *FSTPrefixReader

	count         uint64
	usePrefixDict bool
}

// OpenMPHF opens a prefix index from the given directory. If an FST
// backend file is present, it takes precedence and the classic MPHF
// files are ignored.
func OpenMPHF(outDir string) (*MPHF, error) {
	if FSTPresent(outDir) {
		fst, err := OpenFSTPrefixReader(outDir)
		if err != nil {
			return nil, fmt.Errorf("open fst prefix reader: %w", err)
		}

		return &MPHF{fstIdx: fst, count: fst.Count()}, nil
	}

	mphPath := filepath.Join(outDir, MPHFile)
	combinedPath := filepath.Join(outDir, CombinedMPHFArrayFile)

	info, err := os.Stat(mphPath)
	if err != nil {
		return nil, fmt.Errorf("stat mph file: %w", err)
	}
	if info.Size() == 0 {
		return &MPHF{count: 0}, nil
	}

	mphData, err := os.ReadFile(mphPath)
	if err != nil {
		return nil, fmt.Errorf("read mph file: %w", err)
	}
	mph := &bbhash.BBHash2{}
	if err := mph.UnmarshalBinary(mphData); err != nil {
		return nil, fmt.Errorf("unmarshal MPHF: %w", err)
	}

	combined, err := OpenArrayWithHint(combinedPath, AccessHintRandom)
	if err != nil {
		return nil, fmt.Errorf("open combined fp+pos: %w", err)
	}

	var (
		prefixBlob    *BlobReader
		dictPrefixes  *DictPrefixReader
		usePrefixDict bool
	)
	dictBlobPath := filepath.Join(outDir, PrefixDictBlobFile)
	if _, err := os.Stat(dictBlobPath); err == nil {
		dictPrefixes, err = OpenDictPrefixReader(outDir)
		if err != nil {
			combined.Close()

			return nil, fmt.Errorf("open dict prefixes: %w", err)
		}
		usePrefixDict = true
	} else {
		blobPath := filepath.Join(outDir, PrefixBlobFile)
		offsetsPath := filepath.Join(outDir, PrefixOffsetsFile)
		if _, err := os.Stat(blobPath); err == nil {
			prefixBlob, err = OpenBlob(blobPath, offsetsPath)
			if err != nil {
				combined.Close()

				return nil, fmt.Errorf("open prefix blob: %w", err)
			}
		}
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

// Close releases resources.
func (m *MPHF) Close() error {
	if m.fstIdx != nil {
		return m.fstIdx.Close()
	}
	var combinedErr, blobErr, dictErr error
	if m.combined != nil {
		combinedErr = m.combined.Close()
	}
	if m.prefixBlob != nil {
		blobErr = m.prefixBlob.Close()
	}
	if m.dictPrefixes != nil {
		dictErr = m.dictPrefixes.Close()
	}

	return errors.Join(combinedErr, blobErr, dictErr)
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
	if m.fstIdx != nil {
		return m.fstIdx.GetPrefix(pos)
	}
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

// GetPrefixesAscending reconstructs the prefixes at the given
// positions (sorted ascending). For the FST backend this uses a
// single iterator pass; for the classic backend it dispatches
// per-position GetPrefix calls. Callers should prefer this over a
// hand-rolled per-position loop when the position list is large
// (e.g. a Browse subtree scan).
func (m *MPHF) GetPrefixesAscending(positions []uint64) ([]string, error) {
	if m.fstIdx != nil {
		return m.fstIdx.GetPrefixesAscending(positions)
	}
	out := make([]string, len(positions))
	for i, p := range positions {
		s, err := m.GetPrefix(p)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}

	return out, nil
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
	if m.prefixBlob == nil && m.dictPrefixes == nil && m.fstIdx == nil {
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
