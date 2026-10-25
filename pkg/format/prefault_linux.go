//go:build linux

package format

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"

	"golang.org/x/sys/unix"
)

// Prefault reads the files m lists for the index in dir into the page
// cache before the index serves queries. Each file is mapped, populated
// (MADV_POPULATE_READ, one sequential read) and unmapped; the reader's
// own mappings then take only minor faults. On the benchmark fixture it
// cut opening an evicted index plus its first 1000 lookups by 53%; the
// asynchronous MADV_WILLNEED measured no gain (#55). It reads the whole
// index, so callers use it only for indexes that fit in memory with
// room to spare. On a kernel without MADV_POPULATE_READ (before 5.14) it
// does nothing.
func Prefault(dir string, m *Manifest) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("prefault: %w", err)
	}
	defer root.Close()
	for _, name := range slices.Sorted(maps.Keys(m.Files)) {
		err := prefaultFile(root, name)
		if errors.Is(err, unix.EINVAL) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("prefault: %w", err)
		}
	}

	return nil
}

func prefaultFile(root *os.Root, path string) error {
	f, err := root.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if info.Size() == 0 {
		return nil
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("map %s: %w", path, err)
	}
	err = unix.Madvise(data, unix.MADV_POPULATE_READ)

	return errors.Join(err, unix.Munmap(data))
}
