package format

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// reserveBlocks allocates [from, to) of f and extends it to `to` bytes.
func reserveBlocks(f *os.File, from, to int64) error {
	if err := unix.Fallocate(int(f.Fd()), 0, from, to-from); err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	return nil
}
