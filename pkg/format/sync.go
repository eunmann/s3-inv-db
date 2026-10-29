package format

import (
	"errors"
	"fmt"
	"os"
)

// SyncData makes the data of the file at path durable, with the
// metadata needed to read it back (its size) but not its timestamps.
func SyncData(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	if err := fdatasync(f); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}
