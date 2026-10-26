package jobs

import (
	"sync"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// defaultMaxPending bounds the distinct keys one subscriber holds
// between reads when NewBus is given no limit.
const defaultMaxPending = 256

// Event is one change a Bus carries: a job's newest snapshot, or a
// notice that an inventory's catalog state changed (loaded, unloaded,
// evicted, pinned) outside any job.
type Event struct {
	// Job is the job's snapshot; nil for an inventory notice.
	Job *Job
	// Inventory is the inventory the change is about: the job's
	// inventory, or the one whose catalog state changed.
	Inventory inventory.ID
}

// key identifies what an Event is about, so a newer event for the same
// thing replaces an unread older one.
func (e Event) key() string {
	if e.Job != nil {
		return "job:" + string(e.Job.ID)
	}

	return "inv:" + string(e.Inventory)
}

// Bus fans job snapshots and inventory notices out to subscribers.
// Publish never blocks: each subscriber keeps only the newest unread
// event per job or inventory, so a slow reader skips intermediate
// progress but always ends up with every job's latest state, terminal
// states included. A subscriber that falls behind on more than
// maxPending distinct keys is marked overflowed instead of growing
// without bound; it then has to resynchronise from scratch.
type Bus struct {
	subs       map[*Subscription]struct{}
	maxPending int
	mu         sync.Mutex
}

// NewBus creates a Bus whose subscribers each hold at most maxPending
// unread keys (<= 0 means the default).
func NewBus(maxPending int) *Bus {
	if maxPending <= 0 {
		maxPending = defaultMaxPending
	}

	return &Bus{maxPending: maxPending, subs: make(map[*Subscription]struct{})}
}

// Subscription is one subscriber's coalescing queue. Wait on Ready, then
// call Drain.
type Subscription struct {
	bus     *Bus
	index   map[string]int
	ready   chan struct{}
	queue   []Event
	mu      sync.Mutex
	over    bool
	closed  bool
	limited int
}

// Subscribe registers a new subscriber. Call Close when done.
func (b *Bus) Subscribe() *Subscription {
	s := &Subscription{
		bus:     b,
		index:   make(map[string]int),
		ready:   make(chan struct{}, 1),
		limited: b.maxPending,
	}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()

	return s
}

// Publish broadcasts j's snapshot to every subscriber.
func (b *Bus) Publish(j Job) {
	b.publish(Event{Job: &j, Inventory: j.InventoryID})
}

// PublishInventory tells every subscriber that id's catalog state
// changed outside a job, e.g. the auto-loader evicted it.
func (b *Bus) PublishInventory(id inventory.ID) {
	b.publish(Event{Inventory: id})
}

func (b *Bus) publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		s.offer(ev)
	}
}

func (s *Subscription) offer(ev Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return
	}
	k := ev.key()
	switch i, ok := s.index[k]; {
	case ok:
		s.queue[i] = ev
	case s.over:
		// Already lost track; the reader resynchronises anyway.
	case len(s.queue) >= s.limited:
		s.over = true
		s.queue = nil
		clear(s.index)
	default:
		s.index[k] = len(s.queue)
		s.queue = append(s.queue, ev)
	}
	s.mu.Unlock()
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// Ready is signalled when events are waiting to be drained.
func (s *Subscription) Ready() <-chan struct{} { return s.ready }

// Drain returns the unread events, oldest first change first, and
// empties the queue. The second result reports that events were dropped since
// the last Drain because the reader fell too far behind: the caller
// must treat its view as stale and resynchronise.
func (s *Subscription) Drain() ([]Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs, overflowed := s.queue, s.over
	s.queue, s.over = nil, false
	clear(s.index)

	return evs, overflowed
}

// Close removes the subscription from its bus. Safe to call twice.
func (s *Subscription) Close() {
	s.bus.mu.Lock()
	delete(s.bus.subs, s)
	s.bus.mu.Unlock()
	s.mu.Lock()
	s.closed = true
	s.queue = nil
	s.mu.Unlock()
}
