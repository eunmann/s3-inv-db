package inventory

import (
	"testing"
)

func TestRecordType(t *testing.T) {
	// Basic type check
	r := Record{Key: "test/key.txt", Size: 1024}
	if r.Key != "test/key.txt" {
		t.Errorf("Key = %s, want test/key.txt", r.Key)
	}
	if r.Size != 1024 {
		t.Errorf("Size = %d, want 1024", r.Size)
	}
}
