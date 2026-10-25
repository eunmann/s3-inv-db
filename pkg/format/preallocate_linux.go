package format

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// reserveBlocks allocates [0, size) of f and extends it to size bytes.
func reserveBlocks(f *os.File, size int64) error {
	if err := unix.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	return nil
}
