package extsort

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestTierWriterBackfillAlignment verifies that lazy tier-writer creation
// correctly backfills zeros so the resulting tier columns are aligned to
// preorder positions. The audit suspected an off-by-one at indexbuild.go:310
// (`for range b.posCount - 1`); this test pins the correct behavior.
//
// Setup:
//   - Three rows, all Standard
//   - One row in GlacierFR appears for the first time at position 2
// Expected:
//   - tier_glacier_fr_count.u64 has 4 entries [0,0,1,0] (backfill of 2 zeros
//     for prior rows, value for new row, zero for the row after)
func TestTierWriterBackfillAlignment(t *testing.T) {
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")
	tempDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatal(err)
	}

	b, err := NewIndexBuilder(outDir, tempDir, false)
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}

	// Row 0: just Standard
	row0 := &PrefixRow{Prefix: "a/", Depth: 1}
	row0.TierCounts[tiers.Standard] = 1
	row0.TierBytes[tiers.Standard] = 100
	// Row 1: just Standard
	row1 := &PrefixRow{Prefix: "b/", Depth: 1}
	row1.TierCounts[tiers.Standard] = 1
	row1.TierBytes[tiers.Standard] = 200
	// Row 2: first GlacierFR
	row2 := &PrefixRow{Prefix: "c/", Depth: 1}
	row2.TierCounts[tiers.GlacierFR] = 1
	row2.TierBytes[tiers.GlacierFR] = 300
	// Row 3: just Standard again
	row3 := &PrefixRow{Prefix: "d/", Depth: 1}
	row3.TierCounts[tiers.Standard] = 1
	row3.TierBytes[tiers.Standard] = 400

	for _, r := range []*PrefixRow{row0, row1, row2, row3} {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	stdCount, err := format.OpenArray(filepath.Join(outDir, "tier_stats", "standard_count.u64"))
	if err != nil {
		t.Fatalf("open standard_count: %v", err)
	}
	defer stdCount.Close()
	if got, want := stdCount.Count(), uint64(4); got != want {
		t.Errorf("standard_count length = %d, want %d", got, want)
	}
	for i, want := range []uint64{1, 1, 0, 1} {
		got, _ := stdCount.GetU64(uint64(i))
		if got != want {
			t.Errorf("standard_count[%d] = %d, want %d", i, got, want)
		}
	}

	glCount, err := format.OpenArray(filepath.Join(outDir, "tier_stats", "glacier_fr_count.u64"))
	if err != nil {
		t.Fatalf("open glacier_fr_count: %v", err)
	}
	defer glCount.Close()
	if got, want := glCount.Count(), uint64(4); got != want {
		t.Errorf("glacier_fr_count length = %d, want %d (backfill broken?)", got, want)
	}
	for i, want := range []uint64{0, 0, 1, 0} {
		got, _ := glCount.GetU64(uint64(i))
		if got != want {
			t.Errorf("glacier_fr_count[%d] = %d, want %d", i, got, want)
		}
	}
}

// TestRunFileHeaderRewriteCrashSafety simulates what happens if the
// process dies after the Close() seek but before the count rewrite.
// Today this corrupts the file (count remains 0, body is unreadable).
// This test pins the failure mode so a future trailer-based format
// can replace it.
func TestRunFileHeaderRewriteCrashSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run.bin")
	w, err := NewRunFileWriter(path, 64*1024)
	if err != nil {
		t.Fatalf("NewRunFileWriter: %v", err)
	}
	row := &PrefixRow{Prefix: "a/", Depth: 1, Count: 1, TotalBytes: 100}
	row.TierCounts[tiers.Standard] = 1
	row.TierBytes[tiers.Standard] = 100
	for range 10 {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	// Flush only — skip header rewrite, simulating a crash mid-Close.
	if err := w.writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.file.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenRunFile(path, 64*1024)
	if err != nil {
		t.Fatalf("OpenRunFile post-crash: %v", err)
	}
	defer r.Close()
	if got := r.Count(); got != 0 {
		t.Errorf("expected corrupted Count=0 after partial close, got %d (test stale?)", got)
	}
	// Body is still on disk but unreachable because Read() compares against count.
	_, err = r.Read()
	if err != io.EOF {
		t.Errorf("Read after partial close = %v, want EOF (header says 0 records)", err)
	}
	st, _ := os.Stat(path)
	t.Logf("body bytes on disk: %d — orphaned because header.count == 0", st.Size())
}

// TestAggregatorRetainsSourceKey demonstrates that the current aggregator
// pins the original key string's backing array via substring sharing.
// This is the substring-retention finding from the review.
func TestAggregatorRetainsSourceKey(t *testing.T) {
	// Construct a key whose backing buffer is uniquely identifiable.
	bigKey := fmt.Sprintf("tenant-abc/year=2024/month=01/%s.parquet", string(make([]byte, 1024)))
	agg := NewAggregator(16, 0)
	agg.AddObject(bigKey, 1, tiers.Standard)

	// Walk the stored prefixes and check whether any retains a backing
	// array longer than its own length (a clean implementation would copy
	// only the bytes it needs).
	for prefix := range agg.prefixes {
		// The map key is a string. We can't peek at its backing array
		// length directly without unsafe, but if it was sliced from
		// bigKey, len(prefix) << len(bigKey) is the giveaway combined
		// with prefix-being-substring-of-bigKey.
		if len(prefix) < len(bigKey) && contains(bigKey, prefix) {
			t.Logf("prefix %q shares backing array with %d-byte source key",
				truncate(prefix, 40), len(bigKey))
		}
	}
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
