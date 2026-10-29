package benchutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

func TestDiskPeakSamplerCountsFilesUnderDir(t *testing.T) {
	dir := t.TempDir()
	s := benchutil.StartDiskPeakSampler(dir)
	const size = 1 << 20
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "scratch"), make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := s.Stop()
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got < size {
		t.Errorf("peak = %d, want >= %d", got, size)
	}
}

// A build removes scratch while the sampler walks it: a vanished path
// counts as empty, not as an error.
func TestDiskUsageSkipsRemovedPaths(t *testing.T) {
	n, err := benchutil.DiskUsage(filepath.Join(t.TempDir(), "gone"))
	if err != nil || n != 0 {
		t.Errorf("DiskUsage(removed) = %d, %v; want 0, nil", n, err)
	}
}
