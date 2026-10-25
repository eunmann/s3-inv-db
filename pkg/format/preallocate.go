package format

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateChunk is the zero buffer size writeZeros fills a file with.
const preallocateChunk = 1 << 20

// preallocate grows f from to bytes with every new block
// allocated on disk, so later writes through a mapping cannot fail for
// lack of space (a sparse block written through a mapping on a full
// volume is a SIGBUS, not an error). [0, from) must already be f's
// allocated contents; it is left untouched.
func preallocate(f *os.File, from, to int64) error {
	if to <= from {
		return nil
	}
	err := reserveBlocks(f, from, to)
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		err = writeZeros(f, from, to)
	}
	if err != nil {
		return fmt.Errorf("preallocate bytes %d..%d: %w", from, to, err)
	}

	return nil
}

// writeZeros allocates [from, to) of f by writing it, for filesystems
// with no block reservation call.
func writeZeros(f *os.File, from, to int64) error {
	zeros := make([]byte, min(to-from, preallocateChunk))
	for off := from; off < to; off += int64(len(zeros)) {
		n := min(int64(len(zeros)), to-off)
		if _, err := f.WriteAt(zeros[:n], off); err != nil {
			return fmt.Errorf("write zeros: %w", err)
		}
	}

	return nil
}
