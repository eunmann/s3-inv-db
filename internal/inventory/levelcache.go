package inventory

import (
	"container/list"
	"math"
	"sync"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// LevelCacheMinChildren is the smallest level a LevelCache keeps: a
// smaller level is cheap to order on every view.
const LevelCacheMinChildren = 4096

// levelEntryOverhead approximates an entry's bytes besides its order or
// rows: the entry, its list element and its map slot.
const levelEntryOverhead = 256

// LevelCache keeps the finished display order of large levels, so a
// repeat view of a level (the next page, the same page again) is
// O(log n + page) instead of reading and ordering every child again.
// Indexes are immutable while loaded; entries are keyed by each index's
// Identity, so a reloaded index never sees an old index's entries.
//
// The cache holds at most its byte budget, evicting the least recently
// used levels first. Concurrent requests for the same level share one
// computation. Cost orders depend on the price table, so one cache must
// serve one price table. A nil *LevelCache caches nothing.
type LevelCache struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	lru      list.List // of *levelEntry, most recent first
	entries  map[levelKey]*list.Element
	inflight map[levelKey]*levelCall
}

// levelKey names one ordered level: the index(es), the parent
// position(s) and the query that shaped the order. Browse keys leave b
// nil.
type levelKey struct {
	a, b          *indexread.Identity
	posA, posB    uint64
	okA, okB      bool
	sortBy, dir   string
	hideUnchanged bool
}

// levelEntry is one cached level. A browse entry starts as a marker
// with a nil order, costing only levelEntryOverhead bytes: the first
// view orders only its page, and a second view orders the whole level
// once and keeps it.
type levelEntry struct {
	key    levelKey
	order  []uint32 // browse: span offsets in display order
	rows   []compareRow
	counts CompareStatusCounts
	bytes  int64
}

// levelCall is one in-flight computation that waiters share.
type levelCall struct {
	done  chan struct{}
	entry *levelEntry // nil if the computation produced nothing to keep
}

// NewLevelCache returns a cache holding at most maxBytes of level
// orders, or nil (no caching) when maxBytes is not positive.
func NewLevelCache(maxBytes int64) *LevelCache {
	if maxBytes <= 0 {
		return nil
	}

	return &LevelCache{
		maxBytes: maxBytes,
		entries:  make(map[levelKey]*list.Element),
		inflight: make(map[levelKey]*levelCall),
	}
}

// MaxBytes returns the cache's byte budget; zero for a nil cache.
func (c *LevelCache) MaxBytes() int64 {
	if c == nil {
		return 0
	}

	return c.maxBytes
}

// Bytes returns the bytes the cache holds.
func (c *LevelCache) Bytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.used
}

// browseOrder returns the cached display order of a browse level of n
// children, as span offsets. The first request for a level only marks
// it and reports false, so a level viewed once costs no more than
// ordering its page; the second computes the whole order with compute
// and keeps it.
func (c *LevelCache) browseOrder(key levelKey, n uint64, compute func() []uint32) ([]uint32, bool) {
	if c == nil || n < LevelCacheMinChildren || n > math.MaxUint32 || levelEntryOverhead+int64(n)*4 > c.maxBytes {
		return nil, false
	}
	c.mu.Lock()
	el, ok := c.entries[key]
	if !ok {
		c.insertLocked(&levelEntry{key: key, bytes: levelEntryOverhead})
		c.mu.Unlock()

		return nil, false
	}
	if e := el.Value.(*levelEntry); e.order != nil { //nolint:forcetypeassert // the list holds only *levelEntry
		c.lru.MoveToFront(el)
		c.mu.Unlock()

		return e.order, true
	}
	e := c.computeLocked(key, func() *levelEntry {
		order := compute()

		return &levelEntry{key: key, order: order, bytes: levelEntryOverhead + int64(len(order))*4}
	})
	if e == nil {
		return nil, false
	}

	return e.order, true
}

// compareRows returns the cached rows and counts of a compare level,
// computing them with compute on a miss. Levels with fewer than
// LevelCacheMinChildren children are not kept.
func (c *LevelCache) compareRows(key levelKey, compute func() ([]compareRow, CompareStatusCounts)) ([]compareRow, CompareStatusCounts) {
	if c == nil {
		return compute()
	}
	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.lru.MoveToFront(el)
		e := el.Value.(*levelEntry) //nolint:forcetypeassert // the list holds only *levelEntry
		c.mu.Unlock()

		return e.rows, e.counts
	}
	var rows []compareRow
	var counts CompareStatusCounts
	ran := false
	e := c.computeLocked(key, func() *levelEntry {
		rows, counts = compute()
		ran = true
		if counts.total() < LevelCacheMinChildren {
			return nil
		}

		return &levelEntry{key: key, rows: rows, counts: counts, bytes: levelEntryOverhead + int64(cap(rows))*int64(unsafe.Sizeof(compareRow{}))}
	})
	switch {
	case e != nil:
		return e.rows, e.counts
	case ran:
		return rows, counts
	default:
		// Another request computed a level too small to keep, or its
		// computation failed: compute it here.
		return compute()
	}
}

// computeLocked runs compute once per key across concurrent callers and
// keeps its entry if it fits the budget. Called with c.mu held; returns
// with it released.
func (c *LevelCache) computeLocked(key levelKey, compute func() *levelEntry) *levelEntry {
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		<-call.done

		return call.entry
	}
	call := &levelCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inflight, key)
		if call.entry != nil && call.entry.bytes <= c.maxBytes {
			if el, ok := c.entries[key]; ok {
				c.removeLocked(el)
			}
			c.insertLocked(call.entry)
		}
		c.mu.Unlock()
		close(call.done)
	}()
	call.entry = compute()

	return call.entry
}

// insertLocked adds e as the most recent entry and evicts the least
// recent ones until the cache fits its budget.
func (c *LevelCache) insertLocked(e *levelEntry) {
	c.entries[e.key] = c.lru.PushFront(e)
	c.used += e.bytes
	for c.used > c.maxBytes {
		c.removeLocked(c.lru.Back())
	}
}

func (c *LevelCache) removeLocked(el *list.Element) {
	e := c.lru.Remove(el).(*levelEntry) //nolint:forcetypeassert // the list holds only *levelEntry
	delete(c.entries, e.key)
	c.used -= e.bytes
}

func (c *CompareStatusCounts) total() int {
	return c.Added + c.Removed + c.Changed + c.Unchanged
}
