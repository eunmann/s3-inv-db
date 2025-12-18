package triebuild

import (
	"testing"
)

func TestNodeType(t *testing.T) {
	n := Node{
		Prefix:            "test/",
		Pos:               1,
		Depth:             1,
		SubtreeEnd:        5,
		ObjectCount:       10,
		TotalBytes:        1024,
		MaxDepthInSubtree: 3,
	}
	if n.Prefix != "test/" {
		t.Errorf("Prefix = %s, want test/", n.Prefix)
	}
	if n.Pos != 1 {
		t.Errorf("Pos = %d, want 1", n.Pos)
	}
	if n.Depth != 1 {
		t.Errorf("Depth = %d, want 1", n.Depth)
	}
}
