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
	mph   *bbhash
	slots *MmapFile
	count uint64
	// seed is the key hash's seed; see writeSlots for where it is stored.
	seed     uint64
	slotW    uint64
	fpW      uint8
	posW     uint8
	posOff   uint64
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
	slots, err := OpenMmapWithHint(filepath.Join(outDir, MPHFSlotsFile), AccessHintRandom)
	if err != nil {
		return nil, fmt.Errorf("open MPHF slots: %w", err)
	}
	m := &MPHF{mph: mph, slots: slots, hasSlots: true}
	if err := m.readSlotsHeader(); err != nil {
		return nil, errors.Join(err, slots.Close())
	}

	return m, nil
}

// readSlotsHeader validates mph_slots.bin and reads its layout and seed.
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
	m.fpW, m.posW = data[HeaderSize], data[HeaderSize+1]
	if (m.fpW != 0 && m.fpW != mphfFingerprintBytes) || m.posW < 1 || m.posW > 8 ||
		header.Width != uint32(m.fpW)+uint32(m.posW) {
		return fmt.Errorf("%w: MPHF slot widths %d+%d, slot %d", ErrWidthMismatch, m.fpW, m.posW, header.Width)
	}
	m.count, m.slotW, m.posOff = header.Count, uint64(header.Width), uint64(m.fpW)
	end := uint64(mphfSlotsDataOff) + m.count*m.slotW
	if uint64(len(data)) < end+mphfSeedBytes+mphfSlotsTailPad {
		return fmt.Errorf("read MPHF seed: %w", ErrFileTooSmall)
	}
	m.seed = binary.LittleEndian.Uint64(data[end:])

	return nil
}

// Close releases resources.
func (m *MPHF) Close() error {
	if !m.hasSlots {
		return nil
	}

	return m.slots.Close()
}

// Verified reports whether a Lookup hit is already checked against a
// stored fingerprint. When false, the slots hold positions only and
// the caller must confirm a hit against the stored prefix (indexread
// does, through the prefix tree), which makes every answer exact.
func (m *MPHF) Verified() bool { return m.fpW != 0 }

// Lookup returns the preorder position for a prefix, or ok=false if not
// found. Without fingerprints (see Verified) a prefix that is not in
// the index can return the position of one that is.
func (m *MPHF) Lookup(prefix string) (uint64, bool) {
	if m.count == 0 {
		return 0, false
	}
	slot, ok := m.mph.find(hashString(prefix, m.seed))
	if !ok || slot >= m.count {
		return 0, false
	}
	data := m.slots.Data()
	off := mphfSlotsDataOff + slot*m.slotW
	if m.fpW != 0 && binary.LittleEndian.Uint64(data[off:]) != computeFingerprint(prefix) {
		return 0, false
	}

	return binary.LittleEndian.Uint64(data[off+m.posOff:]) & widthMask[m.posW], true
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
