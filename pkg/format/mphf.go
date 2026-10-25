package format

import (
	"encoding/binary"
	"errors"
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
	mph      *bbhash
	slots    *MmapFile
	count    uint64
	slotW    uint64
	posW     uint8
	hasSlots bool
}

// OpenMPHF opens an MPHF from the given directory.
func OpenMPHF(outDir string) (*MPHF, error) {
	mphPath := filepath.Join(outDir, MPHFile)

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
	slots, err := OpenMmap(filepath.Join(outDir, MPHFSlotsFile))
	if err != nil {
		return nil, fmt.Errorf("open MPHF slots: %w", err)
	}
	m := &MPHF{mph: mph, slots: slots, hasSlots: true}
	if err := m.readSlotsHeader(); err != nil {
		return nil, errors.Join(err, slots.Close())
	}
	if mph.count != m.count {
		return nil, errors.Join(fmt.Errorf("%w: %s holds %d keys, %s %d slots",
			ErrSizeMismatch, MPHFile, mph.count, MPHFSlotsFile, m.count), slots.Close())
	}

	return m, nil
}

// readSlotsHeader validates mph_slots.bin and reads its layout.
func (m *MPHF) readSlotsHeader() error {
	data := m.slots.Data()
	if len(data) < mphfSlotsDataOff {
		return fmt.Errorf("MPHF slots: %w", ErrInvalidHeader)
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return fmt.Errorf("decode MPHF slots header: %w", err)
	}
	if header.Magic != MagicNumber {
		return ErrMagicMismatch
	}
	if header.Version != Version {
		return ErrVersionMismatch
	}
	m.posW = data[HeaderSize]
	if m.posW < 1 || m.posW > 8 || header.Width != mphfFingerprintBytes+uint32(m.posW) {
		return fmt.Errorf("%w: MPHF position width %d, slot %d", ErrWidthMismatch, m.posW, header.Width)
	}
	m.count, m.slotW = header.Count, uint64(header.Width)
	end := uint64(mphfSlotsDataOff) + m.count*m.slotW
	if uint64(len(data)) < end+mphfSlotsTailPad {
		return fmt.Errorf("MPHF slots: %w", ErrFileTooSmall)
	}

	return nil
}

// Close releases resources.
func (m *MPHF) Close() error {
	if !m.hasSlots {
		return nil
	}

	return m.slots.Close()
}

// Lookup returns the preorder position for a prefix, or ok=false if not
// found. A prefix not in the index is misreported only if it lands on a
// slot whose 64-bit fingerprint it also matches (~2^-64 per query).
func (m *MPHF) Lookup(prefix string) (uint64, bool) {
	if m.count == 0 {
		return 0, false
	}
	key := hashString(prefix)
	slot, ok := m.mph.find(key)
	if !ok {
		if len(m.mph.fallback) == 0 {
			return 0, false
		}

		return m.lookupFallback(prefix, key)
	}

	return m.slotPosition(slot, computeFingerprint(prefix))
}

// lookupFallback resolves a key the levels miss through the fallback
// table, which is empty in almost every index.
func (m *MPHF) lookupFallback(prefix string, key uint64) (uint64, bool) {
	fp := computeFingerprint(prefix)
	slot, ok := m.mph.findFallback(key, fp)
	if !ok {
		return 0, false
	}

	return m.slotPosition(slot, fp)
}

// slotPosition returns the position in slot if its fingerprint is fp.
func (m *MPHF) slotPosition(slot, fp uint64) (uint64, bool) {
	if slot >= m.count {
		return 0, false
	}
	data := m.slots.Data()
	off := mphfSlotsDataOff + slot*m.slotW
	if binary.LittleEndian.Uint64(data[off:]) != fp {
		return 0, false
	}

	return binary.LittleEndian.Uint64(data[off+mphfFingerprintBytes:]) & widthMask[m.posW], true
}

// Count returns the number of entries in the MPHF.
func (m *MPHF) Count() uint64 {
	return m.count
}

// hashString is the MPHF key hash of s: wyhash with seed 0.
func hashString(s string) uint64 {
	return wyhash(s, 0)
}

// fingerprintSeed seeds the slot fingerprint. It differs from the key
// hash's seed, so the fingerprint is independent of the key hash that
// chose the slot, and keys that share a key hash still differ in it.
const fingerprintSeed = 0x6a09e667f3bcc909 // the first 64 bits of frac(sqrt(2))

// computeFingerprint is the slot fingerprint of s.
func computeFingerprint(s string) uint64 {
	return wyhash(s, fingerprintSeed)
}
