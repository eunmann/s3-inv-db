package format_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// BenchmarkBuildRaw benchmarks building with raw prefix blob encoding.
func BenchmarkBuildRaw(b *testing.B) {
	benchmarkBuild(b, false)
}

// BenchmarkBuildPrefixDict benchmarks building with dictionary-encoded
// prefix storage.
func BenchmarkBuildPrefixDict(b *testing.B) {
	benchmarkBuild(b, true)
}

func benchmarkBuild(b *testing.B, useDict bool) {
	b.Helper()

	const numPrefixes = 100000
	prefixes := generateDataLakePrefixes(numPrefixes)

	b.ResetTimer()
	for range b.N {
		dir := b.TempDir()
		builder, err := newBuilder(dir, useDict)
		if err != nil {
			b.Fatalf("format.NewStreamingMPHFBuilder failed: %v", err)
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
	benchmarkGetPrefix(b, false)
}

// BenchmarkGetPrefixPrefixDict benchmarks GetPrefix with dictionary-
// encoded prefix storage.
func BenchmarkGetPrefixPrefixDict(b *testing.B) {
	benchmarkGetPrefix(b, true)
}

func benchmarkGetPrefix(b *testing.B, useDict bool) {
	b.Helper()

	const numPrefixes = 100000
	prefixes := generateDataLakePrefixes(numPrefixes)

	dir := b.TempDir()
	builder, err := newBuilder(dir, useDict)
	if err != nil {
		b.Fatalf("format.NewStreamingMPHFBuilder failed: %v", err)
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

	m, err := format.OpenMPHF(dir)
	if err != nil {
		b.Fatalf("format.OpenMPHF failed: %v", err)
	}
	defer m.Close()

	numActual := len(prefixes)
	b.ResetTimer()
	for i := range b.N {
		pos := uint64(i % numActual)
		_, _ = m.GetPrefix(pos)
	}
}

// BenchmarkMPHFLookupRaw benchmarks full lookup with raw encoding.
func BenchmarkMPHFLookupRaw(b *testing.B) {
	benchmarkMPHFLookupEncoding(b, false)
}

// BenchmarkMPHFLookupPrefixDict benchmarks full lookup with dictionary-
// encoded prefix storage.
func BenchmarkMPHFLookupPrefixDict(b *testing.B) {
	benchmarkMPHFLookupEncoding(b, true)
}

func benchmarkMPHFLookupEncoding(b *testing.B, useDict bool) {
	b.Helper()

	const numPrefixes = 100000
	prefixes := generateDataLakePrefixes(numPrefixes)

	dir := b.TempDir()
	builder, err := newBuilder(dir, useDict)
	if err != nil {
		b.Fatalf("format.NewStreamingMPHFBuilder failed: %v", err)
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

	m, err := format.OpenMPHF(dir)
	if err != nil {
		b.Fatalf("format.OpenMPHF failed: %v", err)
	}
	defer m.Close()

	numActual := len(prefixes)
	b.ResetTimer()
	for i := range b.N {
		p := prefixes[i%numActual]
		_, _ = m.Lookup(p)
	}
}

func newBuilder(dir string, useDict bool) (*format.StreamingMPHFBuilder, error) {
	if useDict {
		b, err := format.NewStreamingMPHFBuilder(dir, format.WithPrefixDictionary())
		if err != nil {
			return nil, fmt.Errorf("new streaming mphf (dict): %w", err)
		}

		return b, nil
	}

	b, err := format.NewStreamingMPHFBuilder(dir)
	if err != nil {
		return nil, fmt.Errorf("new streaming mphf (raw): %w", err)
	}

	return b, nil
}

// generateDataLakePrefixes generates n S3 prefixes shaped like
// {org}/{team}/{project}/{env}/{data_type}/{year}/{month}/{day}/{hour}/{batch}/.
func generateDataLakePrefixes(n int) []string {
	orgs := []string{
		"acme-corp", "globex-industries", "initech-solutions",
		"umbrella-analytics", "wayne-enterprises",
	}
	teams := []string{
		"data-engineering", "machine-learning", "analytics",
		"platform", "security", "research",
	}
	projects := []string{
		"customer-insights", "fraud-detection", "recommendation-engine",
		"user-behavior", "inventory-forecast", "pricing-optimization",
		"churn-prediction", "sentiment-analysis",
	}
	environments := []string{"production", "staging", "development"}
	dataTypes := []string{
		"raw-events", "processed-data", "aggregations",
		"model-outputs", "feature-store", "snapshots",
	}
	years := []string{"2023", "2024", "2025"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}

	prefixes := make([]string, 0, n)
	prefixes = append(prefixes, "")

	for _, org := range orgs {
		if len(prefixes) >= n {
			break
		}
		orgPrefix := org + "/"
		prefixes = append(prefixes, orgPrefix)

		for _, team := range teams {
			if len(prefixes) >= n {
				break
			}
			teamPrefix := orgPrefix + team + "/"
			prefixes = append(prefixes, teamPrefix)

			for _, project := range projects {
				if len(prefixes) >= n {
					break
				}
				projectPrefix := teamPrefix + project + "/"
				prefixes = append(prefixes, projectPrefix)

				for _, env := range environments {
					if len(prefixes) >= n {
						break
					}
					envPrefix := projectPrefix + env + "/"
					prefixes = append(prefixes, envPrefix)

					for _, dataType := range dataTypes {
						if len(prefixes) >= n {
							break
						}
						dataPrefix := envPrefix + dataType + "/"
						prefixes = append(prefixes, dataPrefix)

						for _, year := range years {
							if len(prefixes) >= n {
								break
							}
							yearPrefix := dataPrefix + "year=" + year + "/"
							prefixes = append(prefixes, yearPrefix)

							for _, month := range months {
								if len(prefixes) >= n {
									break
								}
								monthPrefix := yearPrefix + "month=" + month + "/"
								prefixes = append(prefixes, monthPrefix)

								for day := 1; day <= 28; day++ {
									if len(prefixes) >= n {
										break
									}
									dayPrefix := fmt.Sprintf("%sday=%02d/", monthPrefix, day)
									prefixes = append(prefixes, dayPrefix)

									if day <= 7 {
										for hour := range 24 {
											if len(prefixes) >= n {
												break
											}
											hourPrefix := fmt.Sprintf("%shour=%02d/", dayPrefix, hour)
											prefixes = append(prefixes, hourPrefix)

											if hour%6 == 0 {
												for batch := range 4 {
													if len(prefixes) >= n {
														break
													}
													batchPrefix := fmt.Sprintf("%sbatch=%d/", hourPrefix, batch)
													prefixes = append(prefixes, batchPrefix)
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if len(prefixes) > n {
		prefixes = prefixes[:n]
	}

	return prefixes
}

// TestSizeComparison compares file sizes between raw and dictionary
// encoding. Run with: go test -v -run TestSizeComparison.
func TestSizeComparison(t *testing.T) {
	const numPrefixes = 100000
	prefixes := generateDataLakePrefixes(numPrefixes)

	t.Logf("Testing with %d prefixes", len(prefixes))

	var totalLen, totalDepth int
	var maxDepth int
	uniqueSegments := make(map[string]struct{})
	for _, p := range prefixes {
		totalLen += len(p)
		depth := countSlashes(p)
		totalDepth += depth
		if depth > maxDepth {
			maxDepth = depth
		}
		for _, seg := range splitSegments(p) {
			uniqueSegments[seg] = struct{}{}
		}
	}

	t.Log("")
	t.Log("=== PREFIX STATISTICS ===")
	t.Logf("Total prefixes: %d", len(prefixes))
	t.Logf("Average prefix length: %.1f bytes", float64(totalLen)/float64(len(prefixes)))
	t.Logf("Average depth: %.1f levels", float64(totalDepth)/float64(len(prefixes)))
	t.Logf("Max depth: %d levels", maxDepth)
	t.Logf("Unique segments: %d", len(uniqueSegments))
	if len(prefixes) > 1 {
		t.Logf("Sample shortest: %q", prefixes[1])
		t.Logf("Sample longest:  %q", prefixes[len(prefixes)-1])
	}

	rawDir := t.TempDir()
	rawBuilder, err := format.NewStreamingMPHFBuilder(rawDir)
	if err != nil {
		t.Fatalf("format.NewStreamingMPHFBuilder (raw) failed: %v", err)
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

	dictDir := t.TempDir()
	dictBuilder, err := format.NewStreamingMPHFBuilder(dictDir, format.WithPrefixDictionary())
	if err != nil {
		t.Fatalf("format.NewStreamingMPHFBuilder (dict) failed: %v", err)
	}
	for i, p := range prefixes {
		if err := dictBuilder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	if err := dictBuilder.Build(dictDir); err != nil {
		t.Fatalf("Build (dict) failed: %v", err)
	}
	dictBuilder.Close()

	rawPrefixSize := fileSize(t, rawDir, "prefix_blob.bin") + fileSize(t, rawDir, "prefix_offsets.u64")
	dictPrefixSize := fileSize(t, dictDir, format.PrefixDictBlobFile) +
		fileSize(t, dictDir, format.PrefixDictOffsetsFile) +
		fileSize(t, dictDir, format.PrefixDictIDsFile) +
		fileSize(t, dictDir, format.PrefixDictOffsetsPerPrefixFile)

	rawTotalSize := totalDirSize(t, rawDir)
	dictTotalSize := totalDirSize(t, dictDir)

	t.Log("=== PREFIX-SPECIFIC STORAGE ===")
	t.Logf("Raw prefix storage: %d bytes", rawPrefixSize)
	t.Logf("Dictionary prefix storage: %d bytes", dictPrefixSize)
	t.Logf("Prefix compression ratio: %.2fx", float64(rawPrefixSize)/float64(dictPrefixSize))
	t.Logf("Prefix space savings: %.1f%%", (1-float64(dictPrefixSize)/float64(rawPrefixSize))*100)

	t.Log("")
	t.Log("=== TOTAL STORAGE ===")
	t.Logf("Raw total storage: %d bytes", rawTotalSize)
	t.Logf("Dictionary total storage: %d bytes", dictTotalSize)
	t.Logf("Total compression ratio: %.2fx", float64(rawTotalSize)/float64(dictTotalSize))
	t.Logf("Total space savings: %.1f%%", (1-float64(dictTotalSize)/float64(rawTotalSize))*100)

	t.Log("")
	t.Log("=== FILE-BY-FILE BREAKDOWN (RAW) ===")
	logDirContents(t, rawDir)

	t.Log("")
	t.Log("=== FILE-BY-FILE BREAKDOWN (DICTIONARY) ===")
	logDirContents(t, dictDir)

	rawM, err := format.OpenMPHF(rawDir)
	if err != nil {
		t.Fatalf("format.OpenMPHF (raw) failed: %v", err)
	}
	defer rawM.Close()

	dictM, err := format.OpenMPHF(dictDir)
	if err != nil {
		t.Fatalf("format.OpenMPHF (dict) failed: %v", err)
	}
	defer dictM.Close()

	for _, p := range prefixes {
		rawPos, rawOk := rawM.Lookup(p)
		dictPos, dictOk := dictM.Lookup(p)

		if rawOk != dictOk {
			t.Errorf("Lookup(%q): raw=%v, dict=%v", p, rawOk, dictOk)
		}
		if rawPos != dictPos {
			t.Errorf("Lookup(%q): rawPos=%d, dictPos=%d", p, rawPos, dictPos)
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

// countSlashes returns the number of '/' characters in a string.
func countSlashes(s string) int {
	count := 0
	for _, c := range s {
		if c == '/' {
			count++
		}
	}

	return count
}

// splitSegments splits a prefix path into its segments.
func splitSegments(prefix string) []string {
	if prefix == "" {
		return nil
	}
	var segments []string
	start := 0
	for i, c := range prefix {
		if c == '/' {
			if i > start {
				segments = append(segments, prefix[start:i])
			}
			start = i + 1
		}
	}
	if start < len(prefix) {
		segments = append(segments, prefix[start:])
	}

	return segments
}
