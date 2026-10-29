package format

import (
	"bytes"
	"os"
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
