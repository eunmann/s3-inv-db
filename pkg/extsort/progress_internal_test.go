package extsort

import (
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// mergedIterator yields n rows, each consuming two input rows, so its
// Remaining (input left) starts at twice the rows it yields: the shape
// of a merge whose inputs repeat every prefix once.
type mergedIterator struct {
	n, i int
}

func (m *mergedIterator) NextOwned() (*PrefixRow, error) {
	if m.i >= m.n {
		return nil, io.EOF
	}
	m.i++

	return &PrefixRow{}, nil
}

func (m *mergedIterator) Recycle([]*PrefixRow) {}

func (m *mergedIterator) Remaining() uint64 { return uint64(2 * (m.n - m.i)) }

// TestBuildProgress_ReachesTotal: index-build progress counts the merge
// input consumed, so it climbs steadily and ends exactly at the total
// even though far fewer prefixes come out than the total counts.
func TestBuildProgress_ReachesTotal(t *testing.T) {
	const rows = int(5*indexBuildProgressInterval + 123)
	prefetch := newPrefetchIterator(&mergedIterator{n: rows})
	defer prefetch.stop()
	var reports [][2]int64
	it := &reportingIterator{
		inner: prefetch,
		total: int64(prefetch.Remaining()),
		onProgress: func(_, total int64) {
			reports = append(reports, [2]int64{total - int64(prefetch.inputRemaining()), total})
		},
		reportEvery: indexBuildProgressInterval,
	}
	for {
		if _, err := it.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if len(reports) < 5 {
		t.Fatalf("got %d reports, want one per %d rows plus the end", len(reports), indexBuildProgressInterval)
	}
	for i := 1; i < len(reports); i++ {
		if reports[i][0] < reports[i-1][0] {
			t.Fatalf("progress went backwards: %v then %v", reports[i-1], reports[i])
		}
	}
	if last := reports[len(reports)-1]; last[0] != last[1] || last[1] != int64(2*rows) {
		t.Fatalf("last report %v, want done == total == %d", last, 2*rows)
	}
}

// TestCountingReadCloser_CountsBytesRead: the CSV path's byte counter
// sees exactly what the reader consumed.
func TestCountingReadCloser_CountsBytesRead(t *testing.T) {
	var total atomic.Int64
	c := &countingReadCloser{ReadCloser: io.NopCloser(strings.NewReader("hello, inventory")), total: &total}
	b, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if c.n != int64(len(b)) || total.Load() != int64(len(b)) {
		t.Fatalf("counted %d (shared %d), read %d", c.n, total.Load(), len(b))
	}
}

// TestManifestBytes: byte progress needs every chunk's size; one
// unknown size falls back to counting chunks.
func TestManifestBytes(t *testing.T) {
	if got := manifestBytes([]s3fetch.ManifestFile{{Size: 3}, {Size: 4}}); got != 7 {
		t.Errorf("manifestBytes = %d, want 7", got)
	}
	if got := manifestBytes([]s3fetch.ManifestFile{{Size: 3}, {Size: 0}}); got != 0 {
		t.Errorf("manifestBytes with an unknown size = %d, want 0", got)
	}
}
