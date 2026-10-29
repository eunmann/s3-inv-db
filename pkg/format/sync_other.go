//go:build !linux

package format

import (
	"fmt"
	"os"
)

func fdatasync(f *os.File) error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	return nil
}
