package jobs

import "sync"

// CancelFunc removes a Bus subscription and closes its events channel.
type CancelFunc func()

// Bus is a tiny fan-out for job snapshots. Publish is non-blocking — a
// slow subscriber drops events rather than stalling Manager.
type Bus struct {
	subs    map[chan Job]struct{}
	bufSize int
	mu      sync.Mutex
}

// NewBus creates a Bus with the given per-subscriber buffer.
func NewBus(bufSize int) *Bus {
	if bufSize <= 0 {
		bufSize = 32
	}

	return &Bus{bufSize: bufSize, subs: make(map[chan Job]struct{})}
}

// Subscribe returns a receive channel and a cancel func. Calling cancel
// removes the subscription and closes the channel.
func (b *Bus) Subscribe() (<-chan Job, CancelFunc) {
	ch := make(chan Job, b.bufSize)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	cancelFn := func() {
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}

	return ch, cancelFn
}

// Publish broadcasts j to every subscriber. Subscribers with full
// buffers miss this event so a slow SSE client can't stall the worker.
func (b *Bus) Publish(j Job) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- j:
		default:
		}
	}
}
