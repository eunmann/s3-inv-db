package indexread_test

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
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

// TestTop_MatchesSortedReference checks TopByBytes and TopByCount
// against sorting every child: metric descending, ties by ascending
// position. Sizes and counts repeat heavily so ties decide most ranks,
// and limits run below, at and above the child count.
func TestTop_MatchesSortedReference(t *testing.T) {
	outDir := setupTestIndex(t)
	r := rand.New(rand.NewPCG(7, 11))
	var csv strings.Builder
	csv.WriteString("Key,Size\n")
	for i := range 300 {
		for j := range 1 + r.IntN(3) {
			fmt.Fprintf(&csv, "c%03d/f%d,%d\n", i, j, 100*(1+r.IntN(4)))
		}
	}
	if err := buildIndexFromCSV(t, outDir, csv.String()); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()
	root, _ := idx.Lookup("")
	children, err := idx.DescendantsAtDepth(root, 1)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		top    func(pos uint64, limit int) ([]indexread.TopResult, error)
		metric func(indexread.Stats) uint64
	}{
		{
			"bytes", func(p uint64, n int) ([]indexread.TopResult, error) { return idx.TopByBytes(p, 1, n) },
			func(s indexread.Stats) uint64 { return s.TotalBytes },
		},
		{
			"count", func(p uint64, n int) ([]indexread.TopResult, error) { return idx.TopByCount(p, 1, n) },
			func(s indexread.Stats) uint64 { return s.ObjectCount },
		},
	} {
		want := make([]indexread.TopResult, 0, len(children))
		for _, pos := range children {
			want = append(want, indexread.TopResult{Pos: pos, Stats: idx.Stats(pos)})
		}
		slices.SortFunc(want, func(a, b indexread.TopResult) int {
			if c := cmp.Compare(tc.metric(b.Stats), tc.metric(a.Stats)); c != 0 {
				return c
			}

			return cmp.Compare(a.Pos, b.Pos)
		})
		for _, limit := range []int{1, 10, 299, 300, 1000} {
			got, err := tc.top(root, limit)
			if err != nil {
				t.Fatal(err)
			}
			n := min(limit, len(want))
			if !slices.Equal(got, want[:n]) {
				t.Fatalf("%s limit %d: got %v, want %v", tc.name, limit, got, want[:n])
			}
		}
	}
}
