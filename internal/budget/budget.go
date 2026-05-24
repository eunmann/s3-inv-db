// Package budget tracks index disk usage against a configurable cap
// and reserves bytes for in-flight loads.
package budget

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// ErrOverBudget is returned by Reserve when the requested bytes won't fit.
var ErrOverBudget = errors.New("over disk budget")

// ErrReservationActive is returned by Reserve when the caller's token
// is already tracking an in-flight reservation.
var ErrReservationActive = errors.New("reservation already active")

// Tracker is the thread-safe budget accounting primitive.
type Tracker struct {
	keys     map[string]uint64
	cap      uint64
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
		cap:      capBytes,
		headroom: headroomBytes,
		keys:     make(map[string]uint64),
	}
}

// Cap returns the configured byte cap. Set once in New and never
// mutated; safe to read without the tracker mutex.
func (t *Tracker) Cap() uint64 { return t.cap }

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
	if committed >= t.cap {
		return 0
	}

	return t.cap - committed
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
	if t.cap == 0 {
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

// MeasureDir returns the on-disk size of an index directory, reading
// manifest.json when present and falling back to a filesystem walk.
func MeasureDir(ctx context.Context, root string) (uint64, error) {
	if root == "" {
		return 0, nil
	}
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("stat %s: %w", root, err)
	}
	if manifest, err := format.ReadManifest(root); err == nil && len(manifest.Files) > 0 {
		return manifest.TotalBytes(), nil
	}
	var total uint64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk entry: %w", err)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("walk cancelled: %w", ctx.Err())
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat entry: %w", err)
		}
		total += uint64(info.Size())

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk %s: %w", root, err)
	}

	return total, nil
}
