package inventory

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

func TestFitsPrefault(t *testing.T) {
	dir := t.TempDir()
	if fitsPrefault(dir, 1<<40) {
		t.Error("prefaulted an index with no manifest")
	}
	for _, name := range []string{format.CoreStatsFile, format.MPHFile} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, 1000), format.FilePerm); err != nil {
			t.Fatal(err)
		}
	}
	if err := format.WriteManifest(dir, 1, 0); err != nil {
		t.Fatal(err)
	}
	m, err := format.ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	size := m.TotalBytes()
	if !fitsPrefault(dir, size*prefaultMemoryShare) {
		t.Errorf("index of %d bytes not prefaulted with %d bytes of memory", size, size*prefaultMemoryShare)
	}
	if fitsPrefault(dir, size*prefaultMemoryShare-1) {
		t.Error("index prefaulted with less than its share of memory")
	}
}
