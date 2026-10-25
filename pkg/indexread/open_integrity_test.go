package indexread_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// TestOpen_RejectsFilesThatDisagreeOnCount: an index whose prefix tree
// comes from a different index (a mixed or damaged file set) fails to
// open instead of answering with the wrong rows.
func TestOpen_RejectsFilesThatDisagreeOnCount(t *testing.T) {
	small := setupTestIndex(t)
	if err := buildIndexFromCSV(t, small, "Key,Size\na/x,1\n"); err != nil {
		t.Fatal(err)
	}
	big := setupTestIndex(t)
	if err := buildIndexFromCSV(t, big, "Key,Size\na/x,1\nb/y,2\nc/z,3\n"); err != nil {
		t.Fatal(err)
	}
	if idx, err := indexread.Open(small); err != nil {
		t.Fatalf("Open(intact index): %v", err)
	} else {
		_ = idx.Close()
	}

	data, err := os.ReadFile(filepath.Join(big, format.PrefixTreeFile))
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(small)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.WriteFile(format.PrefixTreeFile, data, format.FilePerm); err != nil {
		t.Fatal(err)
	}
	idx, err := indexread.Open(small)
	if !errors.Is(err, indexread.ErrIndexInconsistent) {
		if idx != nil {
			_ = idx.Close()
		}
		t.Fatalf("Open(mixed files) err = %v, want ErrIndexInconsistent", err)
	}
}
