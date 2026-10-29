package format

import (
	"math"
	"math/rand/v2"
	"path/filepath"
	"testing"
)

func TestBoundCodes(t *testing.T) {
	values := make([]uint64, 0, 100_013)
	values = append(values, 0, 1, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 1<<40, 1<<40+1, math.MaxUint64-1, math.MaxUint64)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100_000 {
		values = append(values, rng.Uint64()>>rng.UintN(64))
	}
	for _, v := range values {
		down, up := boundCodeDown(v), boundCodeUp(v)
		if got := boundValue(down); got > v {
			t.Fatalf("down(%d) decodes to %d, above it", v, got)
		}
		if got := boundValue(up); got < v {
			t.Fatalf("up(%d) decodes to %d, below it", v, got)
		}
		// Tight: the next code up from down is above v, the next down
		// from up below it, and both are within 2^-10 of v.
		if down < math.MaxUint16 && boundValue(down+1) <= v && boundValue(down+1) != math.MaxUint64 {
			t.Fatalf("down(%d)=%d is not the largest code at most v", v, boundValue(down))
		}
		if up > 0 && boundValue(up-1) >= v {
			t.Fatalf("up(%d)=%d is not the smallest code at least v", v, boundValue(up))
		}
		// The floor and ceiling bracket every value a bound can stand for.
		if up := boundValue(up); MaxFloor(up) > v || MaxFloor(up) > up {
			t.Fatalf("MaxFloor(%d) = %d is above the value %d it stands for", up, MaxFloor(up), v)
		}
		if down := boundValue(down); MinCeil(down) < v || MinCeil(down) < down {
			t.Fatalf("MinCeil(%d) = %d is below the value %d it stands for", down, MinCeil(down), v)
		}
		if v >= 1<<boundMantBits && v < math.MaxUint64>>1 {
			if d := v - boundValue(down); d > v>>boundMantBits {
				t.Fatalf("down(%d) off by %d", v, d)
			}
			if d := boundValue(up) - v; d > v>>boundMantBits {
				t.Fatalf("up(%d) off by %d", v, d)
			}
		}
	}
	for c := range uint16(math.MaxUint16) {
		if boundValue(c) > boundValue(c+1) {
			t.Fatalf("code %d decodes above code %d", c, c+1)
		}
	}
}

// TestDepthBlocksBoundEveryEntry writes a depth index over random
// stats and checks every block bounds each of its rows, and SpanBlocks
// covers any span of one depth exactly.
func TestDepthBlocksBoundEveryEntry(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	type entry struct{ count, bytes uint64 }
	sizes := []int{1, 200, 64, 1500, 777}
	var stats []entry
	dir := t.TempDir()
	w, err := NewDepthIndexWriter(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for depth, n := range sizes {
		for range n {
			e := entry{rng.Uint64() >> rng.UintN(64), rng.Uint64() >> rng.UintN(64)}
			stats = append(stats, e)
			if err := w.Add(uint32(depth), e.count, e.bytes); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Count() != uint64(len(stats)) || d.MaxDepth() != uint32(len(sizes)-1) {
		t.Fatalf("count %d max depth %d, want %d and %d", d.Count(), d.MaxDepth(), len(stats), len(sizes)-1)
	}
	for depth := range uint32(len(sizes)) {
		full := Span{Lo: d.Start(depth), Hi: d.Start(depth + 1)}
		if full.Len() != uint64(sizes[depth]) {
			t.Fatalf("depth %d has %d rows, want %d", depth, full.Len(), sizes[depth])
		}
		for row := full.Lo; row < full.Hi; row++ {
			if got := d.DepthOf(row); got != depth {
				t.Fatalf("DepthOf(%d) = %d, want %d", row, got, depth)
			}
		}
		for range 50 {
			lo := full.Lo + rng.Uint64N(full.Len())
			span := Span{Lo: lo, Hi: lo + 1 + rng.Uint64N(full.Hi-lo)}
			blocks := d.SpanBlocks(depth, span)
			next := span.Lo
			for i := range blocks.Len() {
				e := blocks.Entries(i)
				if e.Lo != next || e.Len() == 0 {
					t.Fatalf("depth %d span %v: block %d covers %v, want it to start at %d", depth, span, i, e, next)
				}
				next = e.Hi
				bb := blocks.Bounds(i)
				for j := e.Lo; j < e.Hi; j++ {
					s := stats[j]
					if s.count < bb.MinCount || s.count > bb.MaxCount || s.bytes < bb.MinBytes || s.bytes > bb.MaxBytes {
						t.Fatalf("depth %d row %d stats %+v outside block bounds %+v", depth, j, s, bb)
					}
				}
			}
			if next != span.Hi {
				t.Fatalf("depth %d span %v: blocks end at %d", depth, span, next)
			}
		}
	}
}

func TestOpenDepthIndexRejectsWrongBlockCount(t *testing.T) {
	dir := t.TempDir()
	w, err := NewDepthIndexWriter(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(0, 1, 1); err != nil {
		t.Fatal(err)
	}
	for range 199 {
		if err := w.Add(1, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// One block too few: 1 + 199 rows need 1 + 4.
	bw, err := NewArrayWriter(filepath.Join(dir, DepthBlocksFile), 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := bw.WriteU64(0); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err := OpenDepthIndex(dir); err == nil {
		d.Close()
		t.Fatal("OpenDepthIndex accepted 4 blocks for depths of 1 and 199 rows")
	}
}
