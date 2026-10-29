package format

import (
	"errors"
	"math"
	"testing"
)

// TestInternerRefusesToWrapSegmentIDs: the 2^32nd distinct segment is an
// error, not ID 0 again (which would make prefix strings wrong).
func TestInternerRefusesToWrapSegmentIDs(t *testing.T) {
	si, err := NewPrefixSegmentInterner(t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = si.Close() })
	if _, err := si.Intern("seen"); err != nil {
		t.Fatal(err)
	}
	si.nextID = math.MaxUint32
	if id, err := si.Intern("seen"); err != nil || id != 0 {
		t.Errorf("Intern(existing) = %d, %v; want 0, nil", id, err)
	}
	if _, err := si.Intern("new"); !errors.Is(err, ErrTooManySegments) {
		t.Errorf("Intern(new) err = %v, want ErrTooManySegments", err)
	}
}
