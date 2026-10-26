package format_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

func writeDepths(t *testing.T, depths ...uint32) (*format.DepthIndex, error) {
	t.Helper()
	dir := t.TempDir()
	w, err := format.NewDepthIndexWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range depths {
		if err := w.Add(d, 1, 1); err != nil {
			_ = w.Abort()

			return nil, fmt.Errorf("add depth %d: %w", d, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	idx, err := format.OpenDepthIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })

	return idx, nil
}

// TestDepthIndexLevels: rows in level order give each depth's first
// row, the depth of every row and the row count.
func TestDepthIndexLevels(t *testing.T) {
	// root; a/, b/; a/x/, a/y/, b/z/
	idx, err := writeDepths(t, 0, 1, 1, 2, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Count() != 6 || idx.MaxDepth() != 2 {
		t.Fatalf("count %d max depth %d, want 6 and 2", idx.Count(), idx.MaxDepth())
	}
	for d, want := range []uint64{0, 1, 3, 6} {
		if got := idx.Start(uint32(d)); got != want {
			t.Errorf("Start(%d) = %d, want %d", d, got, want)
		}
	}
	for row, want := range []uint32{0, 1, 1, 2, 2, 2} {
		if got := idx.DepthOf(uint64(row)); got != want {
			t.Errorf("DepthOf(%d) = %d, want %d", row, got, want)
		}
	}
}

func TestDepthIndexEmpty(t *testing.T) {
	idx, err := writeDepths(t)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Count() != 0 {
		t.Errorf("Count = %d, want 0", idx.Count())
	}
}

func TestDepthIndexWriterRejectsOutOfOrderDepths(t *testing.T) {
	for name, depths := range map[string][]uint32{
		"first not root": {1},
		"skips a depth":  {0, 2},
		"goes back up":   {0, 1, 0},
	} {
		if _, err := writeDepths(t, depths...); !errors.Is(err, format.ErrDepthOrder) {
			t.Errorf("%s: err = %v, want ErrDepthOrder", name, err)
		}
	}
}
