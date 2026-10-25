package format

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// TestPreallocateKeepsExistingData: growing a file reserves only the new
// range, on both the block-reservation and the zero-writing path, and
// leaves the bytes already there alone.
func TestPreallocateKeepsExistingData(t *testing.T) {
	for _, path := range []struct {
		name string
		grow func(f *os.File, from, to int64) error
	}{{"reserve", preallocate}, {"zeros", writeZeros}} {
		t.Run(path.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "grow_*.tmp")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			head := bytes.Repeat([]byte{0xAB}, 10_000)
			if _, err := f.Write(head); err != nil {
				t.Fatal(err)
			}
			const to = 3 << 20
			if err := path.grow(f, int64(len(head)), to); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, to)
			if _, err := f.ReadAt(got, 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got[:len(head)], head) {
				t.Error("existing bytes changed")
			}
			if !bytes.Equal(got[len(head):], make([]byte, to-len(head))) {
				t.Error("grown range is not zero")
			}
			var st unix.Stat_t
			if err := unix.Fstat(int(f.Fd()), &st); err != nil {
				t.Fatal(err)
			}
			if st.Blocks*512 < to {
				t.Errorf("allocated %d bytes on disk, want at least %d", st.Blocks*512, to)
			}
		})
	}
}

// TestCoreStatsBuildFileIsAllocated: the core stats build file, written
// through a mapping, has every block allocated after it grows (#67).
func TestCoreStatsBuildFileIsAllocated(t *testing.T) {
	dir := t.TempDir()
	b, err := NewCoreStatsBuilder(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for range 100_000 { // grows the file several times past its 16-row start
		if err := b.Add(1, 1, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	var st unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, CoreStatsFile), &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 < st.Size {
		t.Errorf("build file is %d bytes with %d allocated", st.Size, st.Blocks*512)
	}
}

// TestCoreStatsBuildFileIgnoresOversizedHint: the capacity hint is an
// over-counted upper bound, so the build file reserves no more than
// coreStatsMaxInitialRows up front; Add grows it as rows arrive.
func TestCoreStatsBuildFileIgnoresOversizedHint(t *testing.T) {
	dir := t.TempDir()
	b, err := NewCoreStatsBuilder(dir, 16*coreStatsMaxInitialRows)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var st unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, CoreStatsFile), &st); err != nil {
		t.Fatal(err)
	}
	limit := int64(HeaderSize) + coreStatsMaxInitialRows*coreStatsBuildStride
	if st.Size > limit || st.Blocks*512 > limit+int64(unix.Getpagesize()) {
		t.Errorf("empty build file is %d bytes with %d allocated, want at most %d", st.Size, st.Blocks*512, limit)
	}
}
