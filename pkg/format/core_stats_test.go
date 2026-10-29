package format_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestCoreStatsAdaptiveWidths_RoundTrip builds a core_stats file with
// known-small observed maxes, asserts the chosen stride is narrower
// than the safe-wide build stride, and round-trips every field
// through the reader. Catches off-by-ones in the writer repack or
// the reader's mask-load offsets.
func TestCoreStatsAdaptiveWidths_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name              string
		count, totalBytes uint64
		tierMask          uint16
		childStart        uint64
		wantStride        format.CoreStatsStride
	}{
		{
			name:  "all_min", // widths should be 1B per field
			count: 1, totalBytes: 1,
			tierMask:   1,
			childStart: 0,
			wantStride: format.CoreStatsStride{Count: 1, Bytes: 1, ChildStart: 1, TierMask: 0},
		},
		{
			name:  "mid_range", // u16 for count/bytes, u24 for child_start
			count: 0xFFFF, totalBytes: 0xFFFF,
			tierMask:   0b101,
			childStart: 0xFFFFFF,
			wantStride: format.CoreStatsStride{Count: 2, Bytes: 2, ChildStart: 3, TierMask: 1},
		},
		{
			name:  "mask_overflows_u8", // a mask past bit 7 takes two bytes
			count: 1, totalBytes: 1,
			tierMask:   1 << 12,
			childStart: 0,
			wantStride: format.CoreStatsStride{Count: 1, Bytes: 1, ChildStart: 1, TierMask: 2},
		},
		{
			name:  "wide", // pessimal — all u64
			count: 1 << 40, totalBytes: 1 << 50,
			tierMask:   0b11,
			childStart: 1 << 33,
			wantStride: format.CoreStatsStride{Count: 6, Bytes: 7, ChildStart: 5, TierMask: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			outDir := filepath.Join(dir, "idx")
			if err := writeOneRow(outDir, tc.count, tc.totalBytes, tc.tierMask, tc.childStart); err != nil {
				t.Fatalf("build: %v", err)
			}

			r, err := format.OpenCoreStats(outDir)
			if err != nil {
				t.Fatalf("OpenCoreStats: %v", err)
			}
			defer r.Close()

			if got := r.Stride(); got != tc.wantStride {
				t.Errorf("stride = %+v, want %+v", got, tc.wantStride)
			}
			if got := r.UnsafeObjectCount(0); got != tc.count {
				t.Errorf("count = %d, want %d", got, tc.count)
			}
			if got := r.UnsafeTotalBytes(0); got != tc.totalBytes {
				t.Errorf("bytes = %d, want %d", got, tc.totalBytes)
			}
			if got := r.UnsafeChildStart(0); got != tc.childStart {
				t.Errorf("child_start = %d, want %d", got, tc.childStart)
			}
			if got := r.UnsafeTierField(0); got != tc.tierMask {
				t.Errorf("tier_mask = %#b, want %#b", got, tc.tierMask)
			}
		})
	}
}

// writeOneRow builds a one-row core_stats file in outDir.
func writeOneRow(outDir string, count, totalBytes uint64, tierMask uint16, childStart uint64) error {
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	b, err := format.NewCoreStatsBuilder(outDir, 1)
	if err != nil {
		return fmt.Errorf("new builder: %w", err)
	}
	if err := b.Add(count, totalBytes, childStart, tierMask); err != nil {
		_ = b.Close()

		return fmt.Errorf("add: %w", err)
	}
	if err := b.Finalize(); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}

// TestCoreStatsFileSize pins the layout: the file is exactly the
// header, the stride descriptor, the packed rows and the tail pad.
func TestCoreStatsFileSize(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "idx")
	if err := writeOneRow(outDir, 3, 300, 0b11, 0); err != nil {
		t.Fatal(err)
	}
	r, err := format.OpenCoreStats(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	info, err := os.Stat(filepath.Join(outDir, format.CoreStatsFile))
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(format.HeaderSize + format.CoreStatsStrideBytes + r.Stride().RowBytes() + 8); info.Size() != want {
		t.Errorf("file size = %d, want %d", info.Size(), want)
	}
}

// BenchmarkCoreStatsFinalize repacks 2M build rows (counts and bytes
// spread over every width, so some rows are patched) into the final
// core stats file: the finalize step that rewrites every row.
func BenchmarkCoreStatsFinalize(b *testing.B) {
	const rows = 2_000_000
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		w, err := format.NewCoreStatsBuilder(b.TempDir(), rows)
		if err != nil {
			b.Fatal(err)
		}
		for i := range uint64(rows) {
			if err := w.Add(i%1000, i*i%(1<<40), i+1, uint16(i%4)); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := w.Finalize(); err != nil {
			b.Fatal(err)
		}
	}
}
