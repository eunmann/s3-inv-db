//go:build linux

//nolint:testpackage // checks page-cache residency of the files Prefault reads
package format

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentPages returns how many of path's pages are in the page cache.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func residentPages(t *testing.T, path string) (int, int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(data)
	vec := make([]byte, (len(data)+os.Getpagesize()-1)/os.Getpagesize())
	if _, _, e := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		t.Fatal(e)
	}
	n := 0
	for _, v := range vec {
		n += int(v & 1)
	}

	return n, len(vec)
}

func TestPrefaultReadsManifestFiles(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, TierStatsDir)
	if err := os.MkdirAll(sub, DirPerm); err != nil {
		t.Fatal(err)
	}
	listed := []string{CoreStatsFile, filepath.Join(TierStatsDir, TierDenseFile)}
	extra := "extra.bin"
	m := &Manifest{Files: map[string]FileInfo{}}
	for _, name := range append([]string{extra}, listed...) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, make([]byte, 4<<20), FilePerm); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Sync()
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		f.Close()
		if name != extra {
			m.Files[name] = FileInfo{Size: 4 << 20}
		}
	}
	if got, all := residentPages(t, filepath.Join(dir, extra)); got == all {
		t.Skip("the kernel kept the evicted pages; nothing to measure")
	}
	if err := Prefault(dir, m); err != nil {
		t.Fatal(err)
	}
	for _, name := range listed {
		if got, all := residentPages(t, filepath.Join(dir, name)); got != all {
			t.Errorf("%s: %d of %d pages resident after Prefault", name, got, all)
		}
	}
	if got, all := residentPages(t, filepath.Join(dir, extra)); got == all {
		t.Errorf("%s is not in the manifest but Prefault read all %d of its pages", extra, all)
	}
}
