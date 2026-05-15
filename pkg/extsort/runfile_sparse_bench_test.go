package extsort

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestSparseRunFileRoundTrip verifies the sparse format preserves all
// data through a write/read cycle.
func TestSparseRunFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse.bin")

	in := []*PrefixRow{
		{Prefix: "a/", Depth: 1, Count: 3, TotalBytes: 1024,
			TierCounts: [MaxTiers]uint64{tiers.Standard: 3},
			TierBytes:  [MaxTiers]uint64{tiers.Standard: 1024}},
		{Prefix: "b/", Depth: 1, Count: 7, TotalBytes: 7000,
			TierCounts: [MaxTiers]uint64{tiers.Standard: 5, tiers.GlacierFR: 2},
			TierBytes:  [MaxTiers]uint64{tiers.Standard: 5000, tiers.GlacierFR: 2000}},
	}

	w, err := NewSparseRunFileWriter(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteAll(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenSparseRunFile(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range in {
		got, err := r.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got.Prefix != want.Prefix || got.Count != want.Count || got.TotalBytes != want.TotalBytes {
			t.Errorf("row %d: got %+v, want %+v", i, got, want)
		}
		for j := range MaxTiers {
			if got.TierCounts[j] != want.TierCounts[j] || got.TierBytes[j] != want.TierBytes[j] {
				t.Errorf("row %d tier %d: count=%d/%d bytes=%d/%d",
					i, j, got.TierCounts[j], want.TierCounts[j], got.TierBytes[j], want.TierBytes[j])
			}
		}
	}
}

// BenchmarkRunFileWriteSparse measures the sparse-format write throughput
// and on-disk size. Rows are typical S3 (Standard-only) shape.
func BenchmarkRunFileWriteSparse(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)

	b.ReportAllocs()
	b.ResetTimer()
	var totalBytes int64
	for i := range b.N {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("run_%d.sparse", i))
		w, err := NewSparseRunFileWriter(path, 4*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		if err := w.WriteAll(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		st, _ := os.Stat(path)
		totalBytes += st.Size()
	}
	b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/file")
	b.ReportMetric(float64(totalBytes)/float64(b.N*N), "bytes/row")
}

// BenchmarkRunFileReadSparse measures the sparse-format read throughput.
func BenchmarkRunFileReadSparse(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)
	path := filepath.Join(b.TempDir(), "bench.sparse")
	w, err := NewSparseRunFileWriter(path, 4*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	if err := w.WriteAll(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := OpenSparseRunFile(path, 4*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Read()
			if err != nil {
				break
			}
		}
		r.Close()
	}
}
