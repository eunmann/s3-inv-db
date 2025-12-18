package indexread

import (
	"testing"
)

func TestStatsType(t *testing.T) {
	s := Stats{
		ObjectCount: 100,
		TotalBytes:  2048,
	}
	if s.ObjectCount != 100 {
		t.Errorf("ObjectCount = %d, want 100", s.ObjectCount)
	}
	if s.TotalBytes != 2048 {
		t.Errorf("TotalBytes = %d, want 2048", s.TotalBytes)
	}
}

func TestIndexLookupNotImplemented(t *testing.T) {
	idx := &Index{}
	_, ok := idx.Lookup("test/")
	if ok {
		t.Error("Lookup should return ok=false for unimplemented index")
	}
}
