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
		depth, tierMask   uint16
		subtreeEnd        uint64
		wantStride        format.CoreStatsStride
	}{
		{
			name:  "all_min", // widths should be 1B per field
			count: 1, totalBytes: 1,
			depth: 1, tierMask: 1,
			subtreeEnd: 0,
			wantStride: format.CoreStatsStride{Count: 1, Bytes: 1, SubtreeEnd: 1, Depth: 1, TierMask: 0},
		},
		{
			name:  "mid_range", // u16 for count/bytes, u24 for subtree_end
			count: 0xFFFF, totalBytes: 0xFFFF,
			depth: 1, tierMask: 0b101,
			subtreeEnd: 0xFFFFFF,
			wantStride: format.CoreStatsStride{Count: 2, Bytes: 2, SubtreeEnd: 3, Depth: 1, TierMask: 1},
		},
		{
			name:  "depth_overflows_u8", // depth=256 forces u16
			count: 1, totalBytes: 1,
			depth: 256, tierMask: 1 << 12,
			subtreeEnd: 0,
			wantStride: format.CoreStatsStride{Count: 1, Bytes: 1, SubtreeEnd: 1, Depth: 2, TierMask: 2},
		},
		{
			name:  "wide", // pessimal — all u64
			count: 1 << 40, totalBytes: 1 << 50,
			depth: 1, tierMask: 0b11,
			subtreeEnd: 1 << 33,
			wantStride: format.CoreStatsStride{Count: 6, Bytes: 7, SubtreeEnd: 5, Depth: 1, TierMask: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			outDir := filepath.Join(dir, "idx")
			if err := writeOneRow(outDir, tc.count, tc.totalBytes, tc.depth, tc.tierMask, tc.subtreeEnd); err != nil {
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
			if got := r.UnsafeSubtreeEnd(0); got != tc.subtreeEnd {
				t.Errorf("subtree_end = %d, want %d", got, tc.subtreeEnd)
			}
			if got := r.UnsafeDepth(0); got != uint32(tc.depth) {
				t.Errorf("depth = %d, want %d", got, tc.depth)
			}
			if got := r.UnsafeTierMask(0); got != tc.tierMask {
				t.Errorf("tier_mask = %#b, want %#b", got, tc.tierMask)
			}
		})
	}
}

// writeOneRow builds a one-row core_stats file in outDir.
func writeOneRow(outDir string, count, totalBytes uint64, depth, tierMask uint16, subtreeEnd uint64) error {
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	b, err := format.NewCoreStatsBuilder(outDir, 1)
	if err != nil {
		return fmt.Errorf("new builder: %w", err)
	}
	if err := b.Add(count, totalBytes, depth, tierMask); err != nil {
		_ = b.Close()

		return fmt.Errorf("add: %w", err)
	}
	if err := b.SetSubtree(0, subtreeEnd); err != nil {
		_ = b.Close()

		return fmt.Errorf("set subtree: %w", err)
	}
	if err := b.Finalize(); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}

// TestCoreStatsRowsStartOnACacheLine pins the data offset: rows start
// at byte 64 whatever the field widths, and the file is exactly the
// header block, the rows and the tail pad.
func TestCoreStatsRowsStartOnACacheLine(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "idx")
	if err := writeOneRow(outDir, 3, 300, 1, 0b11, 0); err != nil {
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
	if want := int64(64 + r.Stride().RowBytes() + 8); info.Size() != want {
		t.Errorf("file size = %d, want %d", info.Size(), want)
	}
}
