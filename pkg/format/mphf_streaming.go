package format

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
)

// StreamingMPHFBuilder builds the minimal perfect hash function that
// maps each prefix to its position, with memory bounded whatever the
// prefix count. Add keeps no prefix: it appends the prefix's key hash,
// fingerprint and position key, 24 bytes, to one of spillBuckets spill
// files in tempDir chosen by the hash's top byte, and counts the hash's
// top maxPartBits bits. Start then splits the keys into partitions by
// their hashes' top bits and builds each on its own, from its spill
// records (see MPHFBuild).
type StreamingMPHFBuilder struct {
	tempDir string
	spill   mphfSpill
	build   *MPHFBuild

	// keyHash and fingerprint, when set, stand in for hashString and
	// computeFingerprint: tests force collisions with them.
	keyHash     func(prefix string) uint64
	fingerprint func(prefix string) uint64

	// budget is the heap the build's workers may hold (SetMemoryBudget).
	budget int64
	// partBits, when not negative, fixes the partition bits, and
	// maxPartKeys is the most keys a chosen partition holds when the
	// bits allow it: test hooks over choosePartBits.
	partBits    int
	maxPartKeys uint64

	count   uint64
	started bool
}

// StepBuildHash is the MPHF build's step as an MPHFBuild's progress
// names it. It counts keys.
const StepBuildHash = "Building the prefix hash"

// ErrMPHFAlreadyBuilt is returned when Start or Build is called more
// than once on the same StreamingMPHFBuilder, or AddKey after it: the
// first build consumes the spill files.
var ErrMPHFAlreadyBuilt = errors.New("StreamingMPHFBuilder.Build already called")

// NewStreamingMPHFBuilder creates a new streaming MPHF builder. TempDir
// holds its spill files, created with the first key.
func NewStreamingMPHFBuilder(tempDir string) *StreamingMPHFBuilder {
	return &StreamingMPHFBuilder{
		tempDir:     tempDir,
		budget:      defaultMPHFBudget,
		partBits:    -1,
		maxPartKeys: maxPartitionKeys,
	}
}

// SetMemoryBudget bounds the heap the build's workers hold: fewer
// workers build at once when the largest partition would take more.
// Zero or less keeps the default (defaultMPHFBudget). Call it before
// Start.
func (b *StreamingMPHFBuilder) SetMemoryBudget(bytes int64) {
	if bytes <= 0 {
		bytes = defaultMPHFBudget
	}
	b.budget = bytes
}

// Position keys: a prefix's position is the first position of its
// depth plus its index within the depth, so a builder that knows only
// the index when it adds a prefix passes both, and Start the depths'
// first positions.
const (
	posLocalBits = 52
	posMaxDepth  = 1<<(64-posLocalBits) - 1
	posLocalMask = 1<<posLocalBits - 1
)

// PositionKey packs a prefix's depth and its index within the depth
// into the position key AddKey takes. Start resolves it against the
// depths' first positions.
func PositionKey(depth int, local uint64) (uint64, error) {
	if depth < 0 || depth > posMaxDepth || local > posLocalMask {
		return 0, fmt.Errorf("%w: depth %d, index %d", ErrMPHFPosition, depth, local)
	}

	return uint64(depth)<<posLocalBits | local, nil
}

// Add adds the next prefix at depth 0 of a build without depths: its
// position is the number of prefixes added before it.
func (b *StreamingMPHFBuilder) Add(prefix string) error {
	posKey, err := PositionKey(0, b.count)
	if err != nil {
		return err
	}
	keyHash, fp := b.KeyHashes(prefix)

	return b.AddKey(keyHash, fp, posKey)
}

// KeyHashes returns the key hash and fingerprint Add stores for prefix,
// for a caller that hashes prefixes itself.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (b *StreamingMPHFBuilder) KeyHashes(prefix string) (uint64, uint64) {
	keyHash, fingerprint := prefixHashes(prefix)
	if b.keyHash != nil {
		keyHash = b.keyHash(prefix)
	}
	if b.fingerprint != nil {
		fingerprint = b.fingerprint(prefix)
	}

	return keyHash, fingerprint
}

// AddKey adds a prefix by the key hash and fingerprint KeyHashes
// returned for it and its position key (PositionKey).
func (b *StreamingMPHFBuilder) AddKey(keyHash, fingerprint, posKey uint64) error {
	if b.started {
		return ErrMPHFAlreadyBuilt
	}
	if err := b.spill.add(b.tempDir, keyHash, fingerprint, posKey); err != nil {
		return err
	}
	b.count++

	return nil
}

// Count returns the number of prefixes added.
func (b *StreamingMPHFBuilder) Count() uint64 {
	return b.count
}

// Start builds mph.bin and mph_slots.bin in outDir from the keys
// added, on workers goroutines at first (Grow adds more), and returns
// at once; Wait ends the build. Starts holds the first position of
// each depth and, last, the key count: a key at depth d and index i
// has position starts[d]+i. Nil starts make a position key the
// position itself. A cancelled ctx fails the build.
func (b *StreamingMPHFBuilder) Start(ctx context.Context, outDir string, starts []uint64, workers int) (*MPHFBuild, error) {
	if b.started {
		return nil, ErrMPHFAlreadyBuilt
	}
	b.started = true
	if err := b.spill.flush(); err != nil {
		return nil, err
	}
	if starts != nil && !slices.Equal(starts[max(0, len(starts)-1):], []uint64{b.count}) {
		return nil, fmt.Errorf("%w: %d depth starts for %d keys", ErrMPHFPosition, len(starts), b.count)
	}
	b.build = startMPHFBuild(ctx, b, outDir, starts, workers)

	return b.build, nil
}

// Build builds the MPHF into outDir on GOMAXPROCS workers.
func (b *StreamingMPHFBuilder) Build(outDir string) error {
	h, err := b.Start(context.Background(), outDir, nil, runtime.GOMAXPROCS(0))
	if err != nil {
		return err
	}

	return h.Wait()
}

// Close ends a build still running, discarding its output, and removes
// the spill files, joining every cleanup error so a failed close or
// removal (typical on a full disk mid-build) still surfaces. A build
// already waited for is left as it is.
func (b *StreamingMPHFBuilder) Close() error {
	if b.build != nil {
		b.build.Abort()
		_ = b.build.Wait() // its own caller gets the error
	}

	return b.spill.close()
}

// Spill files: one per value of a key hash's top byte, each record the
// key hash, fingerprint and position key, little-endian.
const (
	spillBits        = 8
	spillBuckets     = 1 << spillBits
	spillRecordBytes = 24
	// SpillBufBytes buffers each spill file's writes: 4 MiB across the
	// buckets while keys are added.
	spillBufBytes = 16 << 10
)

// mphfSpill is the Add side's spill files and the count of keys by
// their hashes' top maxPartBits bits, from which Start sizes every
// partition before reading any.
type mphfSpill struct {
	dir   string
	files [spillBuckets]spillFile
	hist  []uint64
	err   error // the first failed add; every later add returns it
}

// spillFile is one spill file. Records collect in buf and are written
// out when it is full; a bucket's file stays open from its first key
// until its partition reads it.
type spillFile struct {
	f    *os.File
	path string
	buf  []byte
}

// spillPath is the spill file of bucket b, the keys whose hash's top
// byte is b.
func spillPath(dir string, b int) string {
	return numberedPath(dir, "bucket_", b, spillNameDigits)
}

// partPath is partition p's own spill file, split from its bucket's
// when partitions are finer than buckets.
func partPath(dir string, p int) string {
	return numberedPath(dir, "part_", p, partNameDigits)
}

// The digits of a spill file's number: up to spillBuckets buckets and
// 2^maxPartBits partitions.
const (
	spillNameDigits = 3
	partNameDigits  = 5
)

// numberedPath is the path in dir named prefix and then n in digits
// zero-padded decimal digits, built with one allocation: a build makes
// one per spill file.
func numberedPath(dir, prefix string, n, digits int) string {
	var num [8]byte
	for i := digits - 1; i >= 0; i-- {
		num[i] = byte('0' + n%10)
		n /= 10
	}

	return filepath.Join(dir, prefix) + string(num[:digits])
}

// create creates the file at path, keeping buf if it has one.
func (sf *spillFile) create(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, FilePerm)
	if err != nil {
		return fmt.Errorf("create MPHF spill: %w", err)
	}
	sf.f, sf.path = f, path
	if sf.buf == nil {
		sf.buf = make([]byte, 0, spillBufBytes)
	}

	return nil
}

// room makes room in buf for one more record.
func (sf *spillFile) room() error {
	if len(sf.buf)+spillRecordBytes <= cap(sf.buf) {
		return nil
	}

	return sf.flush()
}

// flush writes buf out to the file.
func (sf *spillFile) flush() error {
	if _, err := sf.f.Write(sf.buf); err != nil {
		return fmt.Errorf("write MPHF spill: %w", err)
	}
	sf.buf = sf.buf[:0]

	return nil
}

// closeFile closes the file, keeping buf for the next one.
func (sf *spillFile) closeFile() error {
	err := sf.f.Close()
	sf.f = nil
	if err != nil {
		return fmt.Errorf("close MPHF spill: %w", err)
	}

	return nil
}

func (s *mphfSpill) add(tempDir string, keyHash, fingerprint, posKey uint64) error {
	if s.err != nil {
		return s.err
	}
	if s.hist == nil {
		dir, err := os.MkdirTemp(tempDir, "mphf-spill-")
		if err != nil {
			s.err = fmt.Errorf("create MPHF spill dir: %w", err)

			return s.err
		}
		s.dir = dir
		s.hist = make([]uint64, 1<<maxPartBits)
	}
	bucket := int(keyHash >> (64 - spillBits))
	sf := &s.files[bucket]
	if sf.f == nil {
		if s.err = sf.create(spillPath(s.dir, bucket)); s.err != nil {
			return s.err
		}
	}
	if s.err = sf.room(); s.err != nil {
		return s.err
	}
	sf.buf = binary.LittleEndian.AppendUint64(sf.buf, keyHash)
	sf.buf = binary.LittleEndian.AppendUint64(sf.buf, fingerprint)
	sf.buf = binary.LittleEndian.AppendUint64(sf.buf, posKey)
	s.hist[keyHash>>(64-maxPartBits)]++

	return nil
}

// flush writes out every spill file's last records and frees its
// buffer; the files stay open for reading.
func (s *mphfSpill) flush() error {
	if s.err != nil {
		return s.err
	}
	for i := range s.files {
		if sf := &s.files[i]; sf.f != nil {
			if err := sf.flush(); err != nil {
				return err
			}
			sf.buf = nil
		}
	}

	return nil
}

// close closes the spill files still open and removes them all.
// Idempotent.
func (s *mphfSpill) close() error {
	var errs []error
	for i := range s.files {
		if f := s.files[i].f; f != nil {
			if err := f.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close MPHF spill: %w", err))
			}
			s.files[i] = spillFile{}
		}
	}
	if s.dir != "" {
		if err := os.RemoveAll(s.dir); err != nil {
			errs = append(errs, fmt.Errorf("remove MPHF spill: %w", err))
		}
		s.dir = ""
	}

	return errors.Join(errs...)
}

// MPHFSlotsFile maps each MPHF slot to its prefix's fingerprint and
// position.
const MPHFSlotsFile = "mph_slots.bin"

// mph_slots.bin layout: Header (Count = slots, Width = slot bytes), a
// 1-byte descriptor (position width), the slots (8-byte fingerprint,
// then the position at that width), and an 8-byte zero tail pad for
// masked loads at the last slot.
const (
	mphfSlotsDescBytes   = 1
	mphfSlotsDataOff     = HeaderSize + mphfSlotsDescBytes
	mphfFingerprintBytes = 8
	mphfSlotsTailPad     = 8
)
