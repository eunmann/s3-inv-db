package format

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkBuildRaw benchmarks building with raw prefix blob encoding.
func BenchmarkBuildRaw(b *testing.B) {
	benchmarkBuild(b, PrefixEncodingRaw)
}

// BenchmarkBuildSegmented benchmarks building with segmented prefix encoding.
func BenchmarkBuildSegmented(b *testing.B) {
	benchmarkBuild(b, PrefixEncodingSegDict)
}

func benchmarkBuild(b *testing.B, enc PrefixEncoding) {
	b.Helper()

	const numPrefixes = 10000
	prefixes := generateHierarchicalPrefixes(numPrefixes)

	b.ResetTimer()
	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(enc))
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// BenchmarkGetPrefixRaw benchmarks GetPrefix with raw blob encoding.
func BenchmarkGetPrefixRaw(b *testing.B) {
	benchmarkGetPrefix(b, PrefixEncodingRaw)
}

// BenchmarkGetPrefixSegmented benchmarks GetPrefix with segmented encoding.
func BenchmarkGetPrefixSegmented(b *testing.B) {
	benchmarkGetPrefix(b, PrefixEncodingSegDict)
}

func benchmarkGetPrefix(b *testing.B, enc PrefixEncoding) {
	b.Helper()

	const numPrefixes = 10000
	prefixes := generateHierarchicalPrefixes(numPrefixes)

	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(enc))
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Build(dir); err != nil {
		builder.Close()
		b.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		b.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	b.ResetTimer()
	for i := range b.N {
		pos := uint64(i % numPrefixes)
		_, _ = m.GetPrefix(pos)
	}
}

// BenchmarkMPHFLookupRaw benchmarks full lookup with raw encoding.
func BenchmarkMPHFLookupRaw(b *testing.B) {
	benchmarkMPHFLookupEncoding(b, PrefixEncodingRaw)
}

// BenchmarkMPHFLookupSegmented benchmarks full lookup with segmented encoding.
func BenchmarkMPHFLookupSegmented(b *testing.B) {
	benchmarkMPHFLookupEncoding(b, PrefixEncodingSegDict)
}

func benchmarkMPHFLookupEncoding(b *testing.B, enc PrefixEncoding) {
	b.Helper()

	const numPrefixes = 10000
	prefixes := generateHierarchicalPrefixes(numPrefixes)

	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(enc))
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Build(dir); err != nil {
		builder.Close()
		b.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		b.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	numActual := len(prefixes)
	b.ResetTimer()
	for i := range b.N {
		p := prefixes[i%numActual]
		_, _ = m.Lookup(p)
	}
}

// generateHierarchicalPrefixes generates S3-like prefixes with shared path components.
func generateHierarchicalPrefixes(n int) []string {
	// Simulate realistic S3 paths like:
	// bucket/year/month/day/hour/
	buckets := []string{"data", "logs", "backups", "archive"}
	years := []string{"2022", "2023", "2024", "2025"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}

	prefixes := make([]string, 0, n)

	// Add root
	prefixes = append(prefixes, "")

	// Generate hierarchical prefixes
	for _, bucket := range buckets {
		prefixes = append(prefixes, bucket+"/")
		for _, year := range years {
			prefixes = append(prefixes, bucket+"/"+year+"/")
			for _, month := range months {
				prefix := bucket + "/" + year + "/" + month + "/"
				prefixes = append(prefixes, prefix)
				// Add day-level prefixes
				for day := 1; day <= 28 && len(prefixes) < n; day++ {
					dayPrefix := fmt.Sprintf("%s%02d/", prefix, day)
					prefixes = append(prefixes, dayPrefix)
				}
				if len(prefixes) >= n {
					break
				}
			}
			if len(prefixes) >= n {
				break
			}
		}
		if len(prefixes) >= n {
			break
		}
	}

	// Trim to exact size
	if len(prefixes) > n {
		prefixes = prefixes[:n]
	}

	return prefixes
}

// TestSizeComparison compares file sizes between raw and segmented encoding.
// Run with: go test -v -run TestSizeComparison.
func TestSizeComparison(t *testing.T) {
	const numPrefixes = 10000
	prefixes := generateHierarchicalPrefixes(numPrefixes)

	// Build with raw encoding
	rawDir := t.TempDir()
	rawBuilder, err := NewStreamingMPHFBuilder(rawDir, WithPrefixEncoding(PrefixEncodingRaw))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder (raw) failed: %v", err)
	}
	for i, p := range prefixes {
		if err := rawBuilder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	if err := rawBuilder.Build(rawDir); err != nil {
		t.Fatalf("Build (raw) failed: %v", err)
	}
	rawBuilder.Close()

	// Build with segmented encoding
	segDir := t.TempDir()
	segBuilder, err := NewStreamingMPHFBuilder(segDir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder (seg) failed: %v", err)
	}
	for i, p := range prefixes {
		if err := segBuilder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	if err := segBuilder.Build(segDir); err != nil {
		t.Fatalf("Build (seg) failed: %v", err)
	}
	segBuilder.Close()

	// Compare sizes
	rawPrefixSize := fileSize(t, rawDir, "prefix_blob.bin") + fileSize(t, rawDir, "prefix_offsets.u64")
	segPrefixSize := fileSize(t, segDir, SegmentsBlobFile) +
		fileSize(t, segDir, SegmentsOffsetsFile) +
		fileSize(t, segDir, PrefixSegIDsFile) +
		fileSize(t, segDir, PrefixSegOffsetsFile)

	// Calculate total directory sizes
	rawTotalSize := totalDirSize(t, rawDir)
	segTotalSize := totalDirSize(t, segDir)

	t.Log("=== PREFIX-SPECIFIC STORAGE ===")
	t.Logf("Raw prefix storage: %d bytes", rawPrefixSize)
	t.Logf("Segmented prefix storage: %d bytes", segPrefixSize)
	t.Logf("Prefix compression ratio: %.2fx", float64(rawPrefixSize)/float64(segPrefixSize))
	t.Logf("Prefix space savings: %.1f%%", (1-float64(segPrefixSize)/float64(rawPrefixSize))*100)

	t.Log("")
	t.Log("=== TOTAL STORAGE ===")
	t.Logf("Raw total storage: %d bytes", rawTotalSize)
	t.Logf("Segmented total storage: %d bytes", segTotalSize)
	t.Logf("Total compression ratio: %.2fx", float64(rawTotalSize)/float64(segTotalSize))
	t.Logf("Total space savings: %.1f%%", (1-float64(segTotalSize)/float64(rawTotalSize))*100)

	t.Log("")
	t.Log("=== FILE-BY-FILE BREAKDOWN (RAW) ===")
	logDirContents(t, rawDir)

	t.Log("")
	t.Log("=== FILE-BY-FILE BREAKDOWN (SEGMENTED) ===")
	logDirContents(t, segDir)

	// Verify both produce correct lookups
	rawM, err := OpenMPHF(rawDir)
	if err != nil {
		t.Fatalf("OpenMPHF (raw) failed: %v", err)
	}
	defer rawM.Close()

	segM, err := OpenMPHF(segDir)
	if err != nil {
		t.Fatalf("OpenMPHF (seg) failed: %v", err)
	}
	defer segM.Close()

	// Verify lookups match
	for _, p := range prefixes {
		rawPos, rawOk := rawM.Lookup(p)
		segPos, segOk := segM.Lookup(p)

		if rawOk != segOk {
			t.Errorf("Lookup(%q): raw=%v, seg=%v", p, rawOk, segOk)
		}
		if rawPos != segPos {
			t.Errorf("Lookup(%q): rawPos=%d, segPos=%d", p, rawPos, segPos)
		}
	}
}

func fileSize(t *testing.T, dir, name string) int64 {
	t.Helper()

	path := filepath.Join(dir, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

// totalDirSize returns the total size of all files in a directory.
func totalDirSize(t *testing.T, dir string) int64 {
	t.Helper()

	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("Info %s: %v", entry.Name(), err)
		}
		total += info.Size()
	}
	return total
}

// logDirContents logs the size of each file in a directory.
func logDirContents(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("Info %s: %v", entry.Name(), err)
		}
		t.Logf("  %s: %d bytes", entry.Name(), info.Size())
	}
}
