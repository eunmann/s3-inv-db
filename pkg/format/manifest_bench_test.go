package format_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// BenchmarkManifest checksums an index-shaped directory (370 MiB over
// five files, the largest several pieces) from the page cache: write
// is WriteManifest, as a build's tail runs it; verify is
// VerifyManifest, as verify and fetch run it.
func BenchmarkManifest(b *testing.B) {
	dir := b.TempDir()
	files := map[string]int{
		format.CoreStatsFile:   160 << 20,
		format.MPHFSlotsFile:   96 << 20,
		format.PrefixTreeFile:  64 << 20,
		format.DepthBlocksFile: 1 << 20,
		format.MPHFile:         49 << 20,
	}
	var total int64
	for name, size := range files {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 2654435761 >> 24)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, format.FilePerm); err != nil {
			b.Fatal(err)
		}
		total += int64(size)
	}

	b.Run("op=write", func(b *testing.B) {
		b.SetBytes(total)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := format.WriteManifest(dir, 1, 1, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("op=verify", func(b *testing.B) {
		if _, err := format.WriteManifest(dir, 1, 1, nil, nil); err != nil {
			b.Fatal(err)
		}
		m, err := format.ReadManifest(dir)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(total)
		b.ReportAllocs()
		for b.Loop() {
			if err := format.VerifyManifest(dir, m); err != nil {
				b.Fatal(err)
			}
		}
	})
}
