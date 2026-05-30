package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// u64StreamArray is an append-only stream of uint64 values backed by a
// zstd-compressed temp file. Unlike u64DiskArray it is never mmap'd
// and exposes only sequential read access via Iterate. Used for the
// per-depth posting lists in DepthIndexBuilder where positions are
// added in preorder (monotonically increasing within a depth) and
// read back exactly once during Build's concatenation pass.
//
// The MPHF builder's hashes / preorderpos / fingerprints arrays
// require Slice() (mmap-backed random access for bbhash and the
// scatter-write inversion) and therefore stay on u64DiskArray —
// those values are also pseudo-random, so compression would not
// pay anyway.
type u64StreamArray struct {
	path    string
	file    *os.File
	encoder *zstd.Encoder
	writer  *bufio.Writer
	count   uint64
}

func newU64StreamArray(dir, namePrefix string) (*u64StreamArray, error) {
	f, err := os.CreateTemp(dir, namePrefix+"_*.u64stream")
	if err != nil {
		return nil, fmt.Errorf("create u64 stream array file: %w", err)
	}
	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		f.Close()
		os.Remove(f.Name())

		return nil, fmt.Errorf("create u64 stream encoder: %w", err)
	}

	return &u64StreamArray{
		path:    f.Name(),
		file:    f,
		encoder: enc,
		writer:  bufio.NewWriterSize(enc, 1024*1024),
	}, nil
}

// Append writes a single uint64 to the backing file. Cheap: 8 bytes
// into a 1 MiB bufio that feeds the zstd encoder.
func (a *u64StreamArray) Append(v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if _, err := a.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("u64 stream array append: %w", err)
	}
	a.count++

	return nil
}

// Count returns the number of uint64 values appended so far.
func (a *u64StreamArray) Count() uint64 { return a.count }

// Iterate finalizes the writer, rewinds the file, opens a fresh zstd
// decoder, and invokes cb for each value in append order. Must be
// called at most once. Caller still owns Close to release the file
// and remove it from disk.
func (a *u64StreamArray) Iterate(cb func(v uint64) error) error {
	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("u64 stream array flush: %w", err)
	}
	a.writer = nil
	if err := a.encoder.Close(); err != nil {
		return fmt.Errorf("u64 stream array close encoder: %w", err)
	}
	a.encoder = nil
	if _, err := a.file.Seek(0, 0); err != nil {
		return fmt.Errorf("u64 stream array seek: %w", err)
	}
	dec, err := zstd.NewReader(a.file)
	if err != nil {
		return fmt.Errorf("u64 stream array decoder: %w", err)
	}
	defer dec.Close()

	reader := bufio.NewReaderSize(dec, 1024*1024)
	var buf [8]byte
	for {
		_, err := io.ReadFull(reader, buf[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("u64 stream array read: %w", err)
		}
		if err := cb(binary.LittleEndian.Uint64(buf[:])); err != nil {
			return err
		}
	}
}

// Close flushes any pending writer state and removes the backing file.
// Idempotent: safe to call after Iterate (which already finalized the
// writer chain) or on the error path before Iterate.
func (a *u64StreamArray) Close() error {
	var errs []error
	if a.writer != nil {
		if err := a.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("u64 stream array flush: %w", err))
		}
		a.writer = nil
	}
	if a.encoder != nil {
		if err := a.encoder.Close(); err != nil {
			errs = append(errs, fmt.Errorf("u64 stream array encoder close: %w", err))
		}
		a.encoder = nil
	}
	if a.file != nil {
		if err := a.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("u64 stream array file close: %w", err))
		}
		a.file = nil
	}
	if a.path != "" {
		if err := os.Remove(a.path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("u64 stream array remove: %w", err))
		}
		a.path = ""
	}

	return errors.Join(errs...)
}
