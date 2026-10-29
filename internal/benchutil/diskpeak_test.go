package benchutil_test

import (
	"math/rand/v2"
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
	// Random bytes so a compressing or hole-punching filesystem still
	// allocates the full size.
	data := make([]byte, size)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "scratch"), data, 0o600); err != nil {
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
