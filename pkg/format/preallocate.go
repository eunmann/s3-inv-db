package format

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateChunk is the zero buffer size writeZeros fills a file with.
const preallocateChunk = 1 << 20

// preallocate sizes f to size bytes with every block allocated on disk,
// so later writes through a mapping cannot fail for lack of space.
func preallocate(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	err := reserveBlocks(f, size)
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		err = writeZeros(f, size)
	}
	if err != nil {
		return fmt.Errorf("preallocate %d bytes: %w", size, err)
	}

	return nil
}

// writeZeros allocates [0, size) of f by writing it, for filesystems
// with no block reservation call.
func writeZeros(f *os.File, size int64) error {
	zeros := make([]byte, min(size, preallocateChunk))
	for off := int64(0); off < size; off += int64(len(zeros)) {
		n := min(int64(len(zeros)), size-off)
		if _, err := f.WriteAt(zeros[:n], off); err != nil {
			return fmt.Errorf("write zeros: %w", err)
		}
	}

	return nil
}
