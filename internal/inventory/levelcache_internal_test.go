package inventory

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testRows(n int) ([]compareRow, CompareStatusCounts) {
	rows := make([]compareRow, n)
	for i := range rows {
		rows[i].posA = uint64(i)
	}

	return rows, CompareStatusCounts{Changed: n}
}

// TestLevelCache_ConcurrentMissComputesOnce: requests for one level
// that arrive while it is being computed wait for that computation.
func TestLevelCache_ConcurrentMissComputesOnce(t *testing.T) {
	c := NewLevelCache(64 << 20)
	var calls atomic.Int32
	release := make(chan struct{})
	compute := func() ([]compareRow, CompareStatusCounts) {
		calls.Add(1)
		<-release

		return testRows(LevelCacheMinChildren)
	}
	key := levelKey{sortBy: "size"}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			rows, counts := c.compareRows(key, compute)
			if len(rows) != LevelCacheMinChildren || counts.Changed != LevelCacheMinChildren {
				t.Errorf("got %d rows, counts %+v", len(rows), counts)
			}
		})
	}
	time.Sleep(50 * time.Millisecond) // let every request reach the cache
	close(release)
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("computed %d times, want 1", n)
	}
	if _, _ = c.compareRows(key, compute); calls.Load() != 1 {
		t.Fatal("a cached level was computed again")
	}
}

// TestLevelCache_BrowsePromotesOnSecondView: the first view only marks
// a level, the second orders and keeps it, later views reuse it.
func TestLevelCache_BrowsePromotesOnSecondView(t *testing.T) {
	c := NewLevelCache(64 << 20)
	var calls int
	compute := func() []uint32 {
		calls++

		return make([]uint32, LevelCacheMinChildren)
	}
	key := levelKey{sortBy: "size"}
	for view, want := range []struct {
		ok    bool
		calls int
	}{{false, 0}, {true, 1}, {true, 1}} {
		_, ok := c.browseOrder(key, LevelCacheMinChildren, compute)
		if ok != want.ok || calls != want.calls {
			t.Fatalf("view %d: ok %v after %d computations, want %v after %d", view, ok, calls, want.ok, want.calls)
		}
	}
	if _, ok := c.browseOrder(levelKey{sortBy: "objects"}, LevelCacheMinChildren-1, compute); ok {
		t.Fatal("a level below the threshold was cached")
	}
}

// TestLevelCache_EvictsLeastRecentlyUsed: over budget, the level used
// longest ago goes first.
func TestLevelCache_EvictsLeastRecentlyUsed(t *testing.T) {
	rows, _ := testRows(LevelCacheMinChildren)
	entry := levelEntryOverhead + int64(cap(rows))*32
	c := NewLevelCache(2*entry + entry/2)
	k1, k2, k3 := levelKey{sortBy: "1"}, levelKey{sortBy: "2"}, levelKey{sortBy: "3"}
	var calls int
	compute := func() ([]compareRow, CompareStatusCounts) {
		calls++

		return testRows(LevelCacheMinChildren)
	}
	for _, k := range []levelKey{k1, k2, k1, k3} {
		c.compareRows(k, compute)
	}
	if calls != 3 {
		t.Fatalf("computed %d times, want 3", calls)
	}
	c.compareRows(k1, compute) // still cached: used after k2
	if calls != 3 {
		t.Fatal("k1 was evicted; the least recently used was k2")
	}
	c.compareRows(k2, compute)
	if calls != 4 {
		t.Fatal("k2 was not evicted")
	}
	if c.Bytes() > c.maxBytes {
		t.Fatalf("holds %d bytes, budget %d", c.Bytes(), c.maxBytes)
	}
}

// TestLevelCache_SmallLevelsNotKept: compare levels below the threshold
// are computed every time.
func TestLevelCache_SmallLevelsNotKept(t *testing.T) {
	c := NewLevelCache(64 << 20)
	var calls int
	for range 3 {
		c.compareRows(levelKey{}, func() ([]compareRow, CompareStatusCounts) {
			calls++

			return testRows(10)
		})
	}
	if calls != 3 || c.Bytes() != 0 {
		t.Fatalf("calls %d bytes %d, want 3 and 0", calls, c.Bytes())
	}
}
