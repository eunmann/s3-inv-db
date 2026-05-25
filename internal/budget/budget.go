// Package budget tracks index disk usage against a configurable cap
// and reserves bytes for in-flight loads.
package budget

import (
	"errors"
	"fmt"
	"sync"
)

// ErrOverBudget is returned by Reserve when the requested bytes won't fit.
var ErrOverBudget = errors.New("over disk budget")

// ErrReservationActive is returned by Reserve when the caller's token
// is already tracking an in-flight reservation.
var ErrReservationActive = errors.New("reservation already active")

// Tracker is the thread-safe budget accounting primitive.
type Tracker struct {
	keys     map[string]uint64
	capBytes uint64
	headroom uint64
	used     uint64
	reserved uint64
	mu       sync.Mutex
}

// New constructs a Tracker. Zero cap disables the budget — Reserve
// becomes a no-op and Available reports 0.
func New(capBytes, headroomBytes uint64) *Tracker {
	if headroomBytes > capBytes {
		headroomBytes = capBytes
	}

	return &Tracker{
		capBytes: capBytes,
		headroom: headroomBytes,
		keys:     make(map[string]uint64),
	}
}

// Cap returns the configured byte cap. Set once in New and never
// mutated; safe to read without the tracker mutex.
func (t *Tracker) Cap() uint64 { return t.capBytes }

// Headroom returns the configured headroom. Set once in New and never
// mutated; safe to read without the tracker mutex.
func (t *Tracker) Headroom() uint64 { return t.headroom }

func (t *Tracker) Used() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.used
}

func (t *Tracker) Reserved() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.reserved
}

// Available returns Cap - Used - Reserved - Headroom, clamped at 0.
func (t *Tracker) Available() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.availableLocked()
}

func (t *Tracker) availableLocked() uint64 {
	committed := t.used + t.reserved + t.headroom
	if committed >= t.capBytes {
		return 0
	}

	return t.capBytes - committed
}

func (t *Tracker) Add(bytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.used += bytes
}

func (t *Tracker) Remove(bytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if bytes >= t.used {
		t.used = 0

		return
	}
	t.used -= bytes
}

// Reserve holds `bytes` against the cap. Must be paired with Release.
func (t *Tracker) Reserve(token string, bytes uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.capBytes == 0 {
		t.keys[token] = 0

		return nil
	}
	if bytes == 0 {
		t.keys[token] = 0

		return nil
	}
	if _, exists := t.keys[token]; exists {
		return fmt.Errorf("%w: %q", ErrReservationActive, token)
	}
	if bytes > t.availableLocked() {
		return ErrOverBudget
	}
	t.keys[token] = bytes
	t.reserved += bytes

	return nil
}

// Release drops a Reserve hold. No-op on an unknown token.
func (t *Tracker) Release(token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	bytes, ok := t.keys[token]
	if !ok {
		return
	}
	delete(t.keys, token)
	if bytes >= t.reserved {
		t.reserved = 0

		return
	}
	t.reserved -= bytes
}
