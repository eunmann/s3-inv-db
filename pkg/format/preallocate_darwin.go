package format

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// reserveBlocks allocates to-from bytes past f's physical end, which is
// from (preallocate's files are always fully allocated), and extends it
// to to bytes.
func reserveBlocks(f *os.File, from, to int64) error {
	store := unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Length: to - from}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &store); err != nil {
		return fmt.Errorf("F_PREALLOCATE: %w", err)
	}
	if err := f.Truncate(to); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	return nil
}
