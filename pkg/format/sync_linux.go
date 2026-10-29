package format

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func fdatasync(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("fdatasync: %w", err)
		}

		return nil
	}
}
