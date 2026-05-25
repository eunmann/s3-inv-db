package indexread_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

func TestTopByBytes_RanksDescending(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/file.txt,100
b/file.txt,500
c/file.txt,250
d/file.txt,1000
e/file.txt,50
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	results, err := idx.TopByBytes(rootPos, 1, 3)
	if err != nil {
		t.Fatalf("TopByBytes failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	wantBytes := []uint64{1000, 500, 250}
	for i, r := range results {
		if r.Stats.TotalBytes != wantBytes[i] {
			t.Errorf("results[%d].TotalBytes = %d, want %d", i, r.Stats.TotalBytes, wantBytes[i])
		}
	}
}

func TestTopByCount_RanksDescending(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
big/a.txt,1
big/b.txt,1
big/c.txt,1
big/d.txt,1
mid/a.txt,1
mid/b.txt,1
small/a.txt,1
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	results, err := idx.TopByCount(rootPos, 1, 2)
	if err != nil {
		t.Fatalf("TopByCount failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	wantCounts := []uint64{4, 2}
	for i, r := range results {
		if r.Stats.ObjectCount != wantCounts[i] {
			t.Errorf("results[%d].ObjectCount = %d, want %d", i, r.Stats.ObjectCount, wantCounts[i])
		}
	}
}

func TestTopByBytes_LimitExceedsDescendants(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/file.txt,100
b/file.txt,200
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	results, err := idx.TopByBytes(rootPos, 1, 100)
	if err != nil {
		t.Fatalf("TopByBytes failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].Stats.TotalBytes != 200 {
		t.Errorf("first.TotalBytes = %d, want 200", results[0].Stats.TotalBytes)
	}
}

func TestTopByBytes_ZeroLimit(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
a/file.txt,100
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	results, err := idx.TopByBytes(rootPos, 1, 0)
	if err != nil {
		t.Fatalf("TopByBytes failed: %v", err)
	}
	if results != nil {
		t.Errorf("results = %v, want nil for zero limit", results)
	}
}

func TestTopFiltered_AppliesFilter(t *testing.T) {
	outDir := setupTestIndex(t)

	csv := `Key,Size
big/a.txt,100
big/b.txt,100
big/c.txt,100
small/file.txt,1000
`
	if err := buildIndexFromCSV(t, outDir, csv); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	rootPos, _ := idx.Lookup("")

	results, err := idx.TopFiltered(rootPos, 1, 10, indexread.TopByCountMetric, indexread.Filter{MinCount: 2})
	if err != nil {
		t.Fatalf("TopFiltered failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1 (only big/ has >=2 objects)", len(results))
	}
	if results[0].Stats.ObjectCount != 3 {
		t.Errorf("results[0].ObjectCount = %d, want 3", results[0].Stats.ObjectCount)
	}
}
