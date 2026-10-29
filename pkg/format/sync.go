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

// SyncDir makes a directory's entries durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	if err := d.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync directory: %w", err), d.Close())
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close directory: %w", err)
	}

	return nil
}
