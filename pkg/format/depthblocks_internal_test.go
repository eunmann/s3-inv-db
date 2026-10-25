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

// TestDepthBlocksBoundEveryEntry builds a depth index over random stats
// and checks every block bounds each of its entries, and SpanBlocks
// covers any span exactly.
func TestDepthBlocksBoundEveryEntry(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	const n = 5_000
	type entry struct{ count, bytes uint64 }
	stats := make([]entry, n)
	depths := make([]uint32, n)
	b := NewDepthIndexBuilder(t.TempDir())
	for pos := range uint64(n) {
		depths[pos] = uint32(rng.IntN(4))
		stats[pos] = entry{rng.Uint64() >> rng.UintN(64), rng.Uint64() >> rng.UintN(64)}
		if err := b.Add(pos, depths[pos], stats[pos].count, stats[pos].bytes); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	if err := b.Build(dir); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for depth := range uint32(4) {
		full, err := d.SubtreeSpan(depth, 0, n-1)
		if err != nil {
			t.Fatal(err)
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
					s := stats[d.PositionAt(j)]
					if s.count < bb.MinCount || s.count > bb.MaxCount || s.bytes < bb.MinBytes || s.bytes > bb.MaxBytes {
						t.Fatalf("depth %d entry %d stats %+v outside block bounds %+v", depth, j, s, bb)
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
	b := NewDepthIndexBuilder(t.TempDir())
	for pos := range uint64(200) {
		if err := b.Add(pos, 0, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	if err := b.Build(dir); err != nil {
		t.Fatal(err)
	}
	// One block too few: 200 entries need 4.
	w, err := NewArrayWriter(filepath.Join(dir, DepthBlocksFile), 8)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := w.WriteU64(0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err := OpenDepthIndex(dir); err == nil {
		d.Close()
		t.Fatal("OpenDepthIndex accepted 3 blocks for 200 entries")
	}
}
