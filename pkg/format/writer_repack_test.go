package format_test

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestRepackArrayWidthU64_RoundTrip exercises the adaptive-width
// repack helper used by DepthIndexBuilder and DictPrefixWriter.
// Confirms that every input value survives the round-trip at the
// chosen target width and that the reader honours Header.Width via
// the mask-load path.
func TestRepackArrayWidthU64_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []uint64
		width  uint8
	}{
		{name: "width_1", values: []uint64{0, 1, 0xFF}, width: 1},
		{name: "width_2", values: []uint64{0, 256, 0xFFFF}, width: 2},
		{name: "width_3", values: []uint64{0, 1 << 16, 0xFFFFFF}, width: 3},
		{name: "width_5", values: []uint64{0, 1 << 32, 1 << 39}, width: 5},
		{name: "width_8", values: []uint64{0, 1 << 50, ^uint64(0)}, width: 8},
		// 64 values forces a row that crosses the bufio chunk boundary
		// in practice; small here, but verifies tail-pad doesn't
		// truncate by writing more than 1 element.
		{name: "many", values: manyU64(64), width: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "src.u64")
			dst := filepath.Join(dir, "dst.u64")

			w, err := format.NewArrayWriter(src, 8)
			if err != nil {
				t.Fatalf("NewArrayWriter: %v", err)
			}
			for _, v := range tc.values {
				if err := w.WriteU64(v); err != nil {
					t.Fatalf("WriteU64: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			if err := format.RepackArrayWidthU64(src, dst, tc.width); err != nil {
				t.Fatalf("RepackArrayWidthU64: %v", err)
			}

			r, err := format.OpenArray(dst)
			if err != nil {
				t.Fatalf("OpenArray: %v", err)
			}
			defer r.Close()
			if r.Width() != uint32(tc.width) {
				t.Errorf("width = %d, want %d", r.Width(), tc.width)
			}
			if r.Count() != uint64(len(tc.values)) {
				t.Errorf("count = %d, want %d", r.Count(), len(tc.values))
			}
			for i, want := range tc.values {
				got := r.UnsafeGetU64(uint64(i))
				if got != want {
					t.Errorf("UnsafeGetU64(%d) = %d, want %d", i, got, want)
				}
			}
		})
	}
}

func manyU64(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i) * 0xAA
	}

	return out
}
