package format

import (
	"errors"
	"os"
	"slices"
	"testing"
)

// drain runs Iterate to completion and collects the visited values.
func drainStreamArray(t *testing.T, a *u64StreamArray) []uint64 {
	t.Helper()
	var got []uint64
	if err := a.Iterate(func(v uint64) error {
		got = append(got, v)

		return nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	return got
}

func TestU64StreamArrayEmpty(t *testing.T) {
	a, err := newU64StreamArray(t.TempDir(), "empty")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	defer a.Close()

	if got := drainStreamArray(t, a); len(got) != 0 {
		t.Errorf("Iterate over empty array: got %v, want nil", got)
	}
	if a.Count() != 0 {
		t.Errorf("Count = %d, want 0", a.Count())
	}
}

func TestU64StreamArraySingle(t *testing.T) {
	a, err := newU64StreamArray(t.TempDir(), "single")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	defer a.Close()

	if err := a.Append(42); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got := drainStreamArray(t, a)
	if !slices.Equal(got, []uint64{42}) {
		t.Errorf("Iterate: got %v, want [42]", got)
	}
}

func TestU64StreamArrayRoundTrip(t *testing.T) {
	a, err := newU64StreamArray(t.TempDir(), "roundtrip")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	defer a.Close()

	want := make([]uint64, 10_000)
	for i := range want {
		want[i] = uint64(i) * 13
		if err := a.Append(want[i]); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if a.Count() != uint64(len(want)) {
		t.Errorf("Count = %d, want %d", a.Count(), len(want))
	}

	got := drainStreamArray(t, a)
	if !slices.Equal(got, want) {
		t.Errorf("Iterate produced %d values, want %d", len(got), len(want))
	}
}

func TestU64StreamArrayIterateTwiceFails(t *testing.T) {
	a, err := newU64StreamArray(t.TempDir(), "twice")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	defer a.Close()

	if err := a.Append(1); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Iterate(func(uint64) error { return nil }); err != nil {
		t.Fatalf("first Iterate: %v", err)
	}
	err = a.Iterate(func(uint64) error { return nil })
	if !errors.Is(err, errU64StreamReused) {
		t.Errorf("second Iterate err = %v, want errU64StreamReused", err)
	}
}

func TestU64StreamArrayCloseWithoutIterate(t *testing.T) {
	a, err := newU64StreamArray(t.TempDir(), "noiter")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	if err := a.Append(7); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent — second Close returns nil and leaves no file behind.
	if err := a.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestU64StreamArrayCloseRemovesFile(t *testing.T) {
	dir := t.TempDir()
	a, err := newU64StreamArray(dir, "remove")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	path := a.path
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file %s should be removed; stat err = %v", path, err)
	}
}

func TestU64StreamArrayShortReadDetected(t *testing.T) {
	// Simulate a truncated frame by Appending N values, then lying
	// about how many we wrote. Iterate finalizes the encoder, decodes
	// what's actually on disk (N values), and must surface
	// errU64StreamShortRead instead of silently succeeding.
	a, err := newU64StreamArray(t.TempDir(), "short")
	if err != nil {
		t.Fatalf("newU64StreamArray: %v", err)
	}
	defer a.Close()

	for i := range uint64(100) {
		if err := a.Append(i); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	// Pretend we Appended 500. The on-disk frame only has 100.
	a.count = 500

	err = a.Iterate(func(uint64) error { return nil })
	if !errors.Is(err, errU64StreamShortRead) {
		t.Errorf("Iterate err = %v, want errU64StreamShortRead", err)
	}
}
