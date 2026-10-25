package format

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// reserveBlocks allocates size bytes past f's physical end and extends
// it to size bytes; preallocate calls it on an empty file.
func reserveBlocks(f *os.File, size int64) error {
	store := unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: size}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &store); err != nil {
		return fmt.Errorf("F_PREALLOCATE: %w", err)
	}
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	return nil
}
