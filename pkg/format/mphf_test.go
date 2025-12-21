package format

import (
	"testing"
)

func TestMPHFBuilderEmpty(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 0 {
		t.Errorf("Count = %d, want 0", m.Count())
	}

	_, ok := m.Lookup("test")
	if ok {
		t.Error("expected Lookup to return false for empty MPHF")
	}
}

func TestMPHFBuilderSimple(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	prefixes := []string{"", "a/", "a/b/", "b/", "c/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	// Build writes the prefix blob in preorder
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != uint64(len(prefixes)) {
		t.Errorf("Count = %d, want %d", m.Count(), len(prefixes))
	}

	// Test lookup of all prefixes
	for _, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)
			continue
		}
		// Verify we can get the prefix back
		stored, err := m.GetPrefix(pos)
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", pos, err)
		}
		if stored != p {
			t.Errorf("GetPrefix(%d) = %q, want %q", pos, stored, p)
		}
	}

	// Test lookup of non-existent prefix
	_, ok := m.Lookup("nonexistent/")
	if ok {
		t.Error("Lookup(nonexistent) should return false")
	}
}

func TestMPHFLookupWithVerify(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	prefixes := []string{"", "x/", "y/", "z/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	// LookupWithVerify should work for existing prefixes
	for _, p := range prefixes {
		pos, ok := m.LookupWithVerify(p)
		if !ok {
			t.Errorf("LookupWithVerify(%q) failed", p)
		}
		_ = pos
	}

	// LookupWithVerify should fail for non-existent prefix
	_, ok := m.LookupWithVerify("missing/")
	if ok {
		t.Error("LookupWithVerify(missing) should return false")
	}
}

func TestMPHFVerify(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	prefixes := []string{"", "foo/", "bar/", "baz/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if err := VerifyMPHF(m); err != nil {
		t.Errorf("VerifyMPHF failed: %v", err)
	}
}

func TestMPHFLarge(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	// Create 1000 unique prefixes
	prefixes := make([]string, 1000)
	for i := range 1000 {
		prefixes[i] = prefixFromInt(i)
	}

	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 1000 {
		t.Errorf("Count = %d, want 1000", m.Count())
	}

	// Verify all lookups
	if err := VerifyMPHF(m); err != nil {
		t.Errorf("VerifyMPHF failed: %v", err)
	}

	// Test some random lookups
	for i := range 100 {
		p := prefixFromInt(i * 10)
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)
		}
		_ = pos
	}
}

func TestMPHFNoFalsePositives(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	prefixes := []string{"alpha/", "beta/", "gamma/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	// These should all fail (not in the set)
	nonExistent := []string{
		"delta/",
		"epsilon/",
		"alpha", // missing trailing slash
		"/alpha",
		"ALPHA/", // case sensitive
		"",
	}

	for _, p := range nonExistent {
		_, ok := m.Lookup(p)
		if ok {
			t.Errorf("Lookup(%q) should return false", p)
		}
	}
}

func TestMPHFUnicode(t *testing.T) {
	dir := t.TempDir()
	b := NewMPHFBuilder()

	prefixes := []string{"", "日本語/", "한국어/", "emoji/🎉/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	for _, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)
			continue
		}
		stored, _ := m.GetPrefix(pos)
		if stored != p {
			t.Errorf("GetPrefix returned %q, want %q", stored, p)
		}
	}
}

func TestComputeFingerprint(t *testing.T) {
	// Same input should give same fingerprint
	fp1 := computeFingerprint("test")
	fp2 := computeFingerprint("test")
	if fp1 != fp2 {
		t.Error("fingerprint not deterministic")
	}

	// Different input should give different fingerprint
	fp3 := computeFingerprint("other")
	if fp1 == fp3 {
		t.Error("different inputs gave same fingerprint")
	}
}

// Helper function to create unique prefix strings.
func prefixFromInt(i int) string {
	// Create a path like "a/b/c/" based on integer
	result := ""
	for i > 0 {
		c := 'a' + rune(i%26)
		result = string(c) + "/" + result
		i /= 26
	}
	if result == "" {
		result = "root/"
	}
	return result
}

func BenchmarkMPHFLookup(b *testing.B) {
	dir := b.TempDir()
	builder := NewMPHFBuilder()

	prefixes := make([]string, 10000)
	for i := range 10000 {
		prefixes[i] = prefixFromInt(i)
		builder.Add(prefixes[i], uint64(i))
	}

	if err := builder.Build(dir); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		b.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	b.ResetTimer()
	for i := range b.N {
		p := prefixes[i%len(prefixes)]
		_, _ = m.Lookup(p)
	}
}

// BenchmarkStreamingMPHFBuild benchmarks the streaming MPHF builder's Build phase
// which includes fingerprint computation and file writing.
func BenchmarkStreamingMPHFBuild(b *testing.B) {
	// Generate test prefixes (similar to realistic S3 paths)
	const numPrefixes = 100000
	prefixes := make([]string, numPrefixes)
	for i := range numPrefixes {
		prefixes[i] = prefixFromInt(i)
	}

	b.ResetTimer()
	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		// Add all prefixes
		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		// Build (includes fingerprint computation)
		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// BenchmarkStreamingMPHFBuild500K benchmarks with 500K prefixes for more realistic timing.
func BenchmarkStreamingMPHFBuild500K(b *testing.B) {
	// Generate test prefixes
	const numPrefixes = 500000
	prefixes := make([]string, numPrefixes)
	for i := range numPrefixes {
		prefixes[i] = prefixFromInt(i)
	}

	b.ResetTimer()
	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
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
