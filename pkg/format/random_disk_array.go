package format

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// errRandomDiskArrayBadIdx fires on a write whose index is past the
// current Len() — caller is expected to Grow first. Sentinel for err113.
var errRandomDiskArrayBadIdx = errors.New("random disk array index out of range")

// randomDiskBytes is a file-backed byte buffer with random read/write
// access. Created at some initial capacity, growable on demand, exposed
// as a []byte that aliases the mmap region directly.
//
// Unlike u64DiskArray (which is append-only and only mmaps on Freeze),
// randomDiskBytes is mmap'd from creation so the IndexBuilder can write
// at arbitrary indices (out-of-preorder, since subtrees close out of
// order). The backing file is anonymous-temp; Close removes it.
//
// Memory accounting: the OS counts mmap pages against the *backing file*
// not the Go heap, so a 10 GB random-access "slice" doesn't count
// against GOMEMLIMIT and pages out cleanly under pressure.
type randomDiskBytes struct {
	path string
	file *os.File
	data []byte // aliases mmap region
	size int64  // ftruncate'd size = len(data)
}

func newRandomDiskBytes(dir, namePrefix string, initialBytes int64) (*randomDiskBytes, error) {
	f, err := os.CreateTemp(dir, namePrefix+"_*.mmapdisk")
	if err != nil {
		return nil, fmt.Errorf("create random disk bytes file: %w", err)
	}
	a := &randomDiskBytes{path: f.Name(), file: f}
	if initialBytes <= 0 {
		initialBytes = int64(unix.Getpagesize())
	}
	if err := a.grow(initialBytes); err != nil {
		_ = a.Close()

		return nil, err
	}

	return a, nil
}

// grow extends (or initially sizes) the backing file and re-mmaps it.
// Caller-supplied newSize is the absolute size we want, not a delta.
func (a *randomDiskBytes) grow(newSize int64) error {
	if newSize <= a.size {
		return nil
	}
	if a.data != nil {
		if err := unix.Munmap(a.data); err != nil {
			return fmt.Errorf("random disk bytes munmap: %w", err)
		}
		a.data = nil
	}
	if err := a.file.Truncate(newSize); err != nil {
		return fmt.Errorf("random disk bytes truncate: %w", err)
	}
	data, err := unix.Mmap(int(a.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("random disk bytes mmap: %w", err)
	}
	a.data = data
	a.size = newSize

	return nil
}

// Close munmaps, closes the file, and removes it. Idempotent.
func (a *randomDiskBytes) Close() error {
	var firstErr error
	if a.data != nil {
		if err := unix.Munmap(a.data); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("random disk bytes munmap: %w", err)
		}
		a.data = nil
	}
	if a.file != nil {
		if err := a.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("random disk bytes close: %w", err)
		}
		a.file = nil
	}
	if a.path != "" {
		if err := os.Remove(a.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = fmt.Errorf("random disk bytes remove: %w", err)
		}
		a.path = ""
	}

	return firstErr
}

// U64RandomDiskArray is a file-backed []uint64 with random read/write
// access. Use Append to grow logically, Set to write at an existing
// index. Slice() returns the current values aliased over mmap'd memory.
//
// Designed for the IndexBuilder's subtreeEnds: positions are assigned
// in preorder (append) but values are filled in subtree-close order
// (random write by pos).
type U64RandomDiskArray struct {
	bytes  *randomDiskBytes
	values []uint64 // unsafe view over bytes.data
	length int      // logical count of uint64 entries
}

// NewU64RandomDiskArray creates a u64 array under dir, pre-sized to hold
// capacity entries (no growth needed unless len exceeds capacity).
func NewU64RandomDiskArray(dir, namePrefix string, capacity uint64) (*U64RandomDiskArray, error) {
	cap8 := max(int64(capacity)*8, int64(unix.Getpagesize()))
	bytes, err := newRandomDiskBytes(dir, namePrefix, cap8)
	if err != nil {
		return nil, err
	}
	a := &U64RandomDiskArray{bytes: bytes}
	a.rebindValues()

	return a, nil
}

func (a *U64RandomDiskArray) rebindValues() {
	if len(a.bytes.data) == 0 {
		a.values = nil

		return
	}
	count := len(a.bytes.data) / 8
	a.values = unsafe.Slice((*uint64)(unsafe.Pointer(&a.bytes.data[0])), count)
}

// Append extends the logical length by one and writes v at the new
// position. Grows the backing file (doubling) if needed.
func (a *U64RandomDiskArray) Append(v uint64) error {
	if a.length >= len(a.values) {
		newCount := max(int64(len(a.values))*2, int64(a.length+1))
		if err := a.bytes.grow(newCount * 8); err != nil {
			return err
		}
		a.rebindValues()
	}
	a.values[a.length] = v
	a.length++

	return nil
}

// Set writes v at idx; idx must be < Len().
func (a *U64RandomDiskArray) Set(idx int, v uint64) error {
	if idx < 0 || idx >= a.length {
		return fmt.Errorf("%w: idx=%d len=%d", errRandomDiskArrayBadIdx, idx, a.length)
	}
	a.values[idx] = v

	return nil
}

// Len returns the logical entry count.
func (a *U64RandomDiskArray) Len() int { return a.length }

// Slice returns the live []uint64 view. Valid until next grow / Close.
// Callers may read or write; the underlying mmap is PROT_WRITE.
func (a *U64RandomDiskArray) Slice() []uint64 { return a.values[:a.length] }

// Close releases the mmap and deletes the backing file.
func (a *U64RandomDiskArray) Close() error {
	if a.bytes == nil {
		return nil
	}
	err := a.bytes.Close()
	a.bytes = nil
	a.values = nil

	return err
}

// U16RandomDiskArray is the uint16 sibling of U64RandomDiskArray. Same
// semantics, half the per-entry footprint.
type U16RandomDiskArray struct {
	bytes  *randomDiskBytes
	values []uint16
	length int
}

// NewU16RandomDiskArray creates a u16 array under dir, pre-sized for
// capacity entries.
func NewU16RandomDiskArray(dir, namePrefix string, capacity uint64) (*U16RandomDiskArray, error) {
	cap2 := max(int64(capacity)*2, int64(unix.Getpagesize()))
	bytes, err := newRandomDiskBytes(dir, namePrefix, cap2)
	if err != nil {
		return nil, err
	}
	a := &U16RandomDiskArray{bytes: bytes}
	a.rebindValues()

	return a, nil
}

func (a *U16RandomDiskArray) rebindValues() {
	if len(a.bytes.data) == 0 {
		a.values = nil

		return
	}
	count := len(a.bytes.data) / 2
	a.values = unsafe.Slice((*uint16)(unsafe.Pointer(&a.bytes.data[0])), count)
}

// Append extends the logical length by one and writes v.
func (a *U16RandomDiskArray) Append(v uint16) error {
	if a.length >= len(a.values) {
		newCount := max(int64(len(a.values))*2, int64(a.length+1))
		if err := a.bytes.grow(newCount * 2); err != nil {
			return err
		}
		a.rebindValues()
	}
	a.values[a.length] = v
	a.length++

	return nil
}

// Set writes v at idx; idx must be < Len().
func (a *U16RandomDiskArray) Set(idx int, v uint16) error {
	if idx < 0 || idx >= a.length {
		return fmt.Errorf("%w: idx=%d len=%d", errRandomDiskArrayBadIdx, idx, a.length)
	}
	a.values[idx] = v

	return nil
}

// Len returns the logical entry count.
func (a *U16RandomDiskArray) Len() int { return a.length }

// Slice returns the live []uint16 view.
func (a *U16RandomDiskArray) Slice() []uint16 { return a.values[:a.length] }

// Close releases the mmap and deletes the backing file.
func (a *U16RandomDiskArray) Close() error {
	if a.bytes == nil {
		return nil
	}
	err := a.bytes.Close()
	a.bytes = nil
	a.values = nil

	return err
}
