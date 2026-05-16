package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// errU64DiskArrayMisaligned fires when the backing file's byte length
// isn't a multiple of 8 — indicates a corrupt write or a bug, not a
// runtime condition.
var errU64DiskArrayMisaligned = errors.New("u64 disk array file size not a multiple of 8")

// u64DiskArray is an append-only sequence of uint64 backed by a file
// on disk. During the "write" phase callers Append values through a
// buffered writer; once Freeze is called the file is mmap'd and the
// values become randomly accessible via Slice as []uint64.
//
// The point of routing through disk is to keep large hash/position/
// fingerprint arrays out of the Go heap during a streaming MPHF build.
// At 1B prefixes a single []uint64 in heap is 8 GiB; with the disk
// array the same data lives in the OS page cache (paged in/out
// transparently, doesn't count against GOMEMLIMIT, survives
// MADV_DONTNEED if pressure mounts).
type u64DiskArray struct {
	path   string
	file   *os.File
	writer *bufio.Writer

	// Set by Freeze:
	mmap   []byte // raw mmap
	values []uint64
	count  uint64
}

// newU64DiskArray creates a fresh disk-backed array under dir with a
// human-meaningful prefix in the filename.
func newU64DiskArray(dir, namePrefix string) (*u64DiskArray, error) {
	f, err := os.CreateTemp(dir, namePrefix+"_*.u64disk")
	if err != nil {
		return nil, fmt.Errorf("create u64 disk array file: %w", err)
	}

	return &u64DiskArray{
		path:   f.Name(),
		file:   f,
		writer: bufio.NewWriterSize(f, 1024*1024),
	}, nil
}

// Append writes a single uint64 to the backing file.
func (a *u64DiskArray) Append(v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if _, err := a.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("u64 disk array append: %w", err)
	}
	a.count++

	return nil
}

// Count returns the number of uint64 values appended so far.
func (a *u64DiskArray) Count() uint64 { return a.count }

// Freeze flushes pending writes and mmap's the file read-only. After
// Freeze the array is no longer appendable; Slice returns the values.
// Caller must invoke Close to release the mmap and remove the file.
func (a *u64DiskArray) Freeze() error {
	if a.values != nil {
		// Idempotent.
		return nil
	}
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("u64 disk array flush: %w", err)
	}
	// Sync so the mmap sees the bytes; otherwise on some kernels the
	// page cache may lag and the mmap shows zeros.
	if err := a.file.Sync(); err != nil {
		return fmt.Errorf("u64 disk array sync: %w", err)
	}
	info, err := a.file.Stat()
	if err != nil {
		return fmt.Errorf("u64 disk array stat: %w", err)
	}
	size := info.Size()
	if size == 0 {
		a.values = nil

		return nil
	}
	if size%8 != 0 {
		return fmt.Errorf("%w: got %d", errU64DiskArrayMisaligned, size)
	}
	data, err := unix.Mmap(int(a.file.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("u64 disk array mmap: %w", err)
	}
	// bbhash and the lookup loops read these arrays at random hash
	// positions — pages aren't accessed sequentially.
	_ = unix.Madvise(data, unix.MADV_RANDOM)
	a.mmap = data
	// Re-interpret the mmap'd bytes as a []uint64. Safe because the
	// region is naturally 8-aligned (mmap pages always are) and we've
	// already checked the length is a multiple of 8. The slice header
	// references the mmap region directly; no copy.
	count := int(size / 8)
	a.values = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), count)

	return nil
}

// Slice returns the frozen array as []uint64. Caller must not modify
// the contents. The slice references mmap'd memory and is valid only
// until Close.
func (a *u64DiskArray) Slice() []uint64 { return a.values }

// Close releases the mmap (if any) and removes the backing file.
// Idempotent.
func (a *u64DiskArray) Close() error {
	var firstErr error
	if a.mmap != nil {
		if err := unix.Munmap(a.mmap); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("u64 disk array munmap: %w", err)
		}
		a.mmap = nil
		a.values = nil
	}
	if a.file != nil {
		if err := a.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("u64 disk array close: %w", err)
		}
		a.file = nil
	}
	if a.path != "" {
		if err := os.Remove(a.path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = fmt.Errorf("u64 disk array remove: %w", err)
		}
		a.path = ""
	}

	return firstErr
}
