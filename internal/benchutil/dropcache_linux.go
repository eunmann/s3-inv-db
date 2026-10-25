//go:build linux

package benchutil

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// DropPageCache evicts every regular file under dir from the page
// cache, so the next read pays storage latency. Linux skips a page any
// mapping still holds, so unmap the files (close the index) first.
func DropPageCache(tb testing.TB, dir string) {
	tb.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		tb.Fatalf("drop page cache: %v", err)
	}
	defer root.Close()
	err = fs.WalkDir(root.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		f, err := root.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
			return fmt.Errorf("fadvise %s: %w", path, err)
		}

		return nil
	})
	if err != nil {
		tb.Fatalf("drop page cache: %v", err)
	}
}
