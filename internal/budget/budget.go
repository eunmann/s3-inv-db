// Package budget tracks on-disk usage of materialised inventory
// indexes against a configurable cap and accounts for in-flight load
// reservations so we don't overrun while a download is mid-flight.
//
// Tracker maintains two counters in memory:
//
//   - used: sum of IndexBytes across loaded inventories (set by callers
//     after a load completes via Add, and via Remove on unload)
//   - reserved: sum of in-flight reservations (set by Reserve, released
//     by Release)
//
// Available() = Cap - Used - Reserved. Callers consult Available before
// starting a load; if it's insufficient, they call the Planner to find
// an eviction plan. Reservations are in-memory only — a server restart
// wipes them, which is fine because no load is in-flight at startup.
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

// ErrOverBudget is returned by Reserve when the requested bytes won't
// fit even after accounting for current used+reserved.
var ErrOverBudget = errors.New("over disk budget")

// Tracker is the thread-safe budget accounting primitive.
type Tracker struct {
	mu       sync.Mutex
	cap      uint64
	headroom uint64
	used     uint64
	reserved uint64
	keys     map[string]uint64 // active reservations keyed by load token
}

// New constructs a Tracker with the given absolute byte cap and
// headroom. Cap is the hard ceiling — Used+Reserved never exceeds it.
// Headroom is reserved unused space (subtracted from Available) so an
// in-flight load that exceeds its estimate has room to grow. Both are
// in bytes; a zero cap disables tracking (Available always 0,
// Reserve always returns ErrOverBudget unless requested == 0).
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

// Cap returns the configured byte cap.
func (t *Tracker) Cap() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cap
}

// Headroom returns the configured headroom bytes.
func (t *Tracker) Headroom() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.headroom
}

// Used returns the bytes currently attributable to loaded indexes.
func (t *Tracker) Used() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.used
}

// Reserved returns the bytes held by in-flight load reservations.
func (t *Tracker) Reserved() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reserved
}

// Available returns Cap - Used - Reserved - Headroom, clamped to 0.
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

// Add accounts for an inventory's on-disk size once a load completes.
// `key` is informational — `used` is tracked as a single counter, so
// callers that need to update an entry must issue Remove(key, prior)
// before Add(key, new).
func (t *Tracker) Add(_ string, bytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.used += bytes
}

// Remove subtracts an inventory's on-disk size when it's unloaded or
// evicted. Clamped at zero so accidental double-Remove doesn't
// underflow.
func (t *Tracker) Remove(_ string, bytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if bytes >= t.used {
		t.used = 0
		return
	}
	t.used -= bytes
}

// Reserve places a hold on `bytes` for an in-flight load identified by
// `token`. Returns ErrOverBudget when there isn't room. A successful
// Reserve must be paired with exactly one Release(token) — typically
// in a defer.
func (t *Tracker) Reserve(token string, bytes uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if bytes == 0 {
		t.keys[token] = 0
		return nil
	}
	if t.cap == 0 {
		return ErrOverBudget
	}
	if _, exists := t.keys[token]; exists {
		return fmt.Errorf("reservation %q already active", token)
	}
	if bytes > t.availableLocked() {
		return ErrOverBudget
	}
	t.keys[token] = bytes
	t.reserved += bytes
	return nil
}

// Release drops the in-flight reservation for token. Safe to call on
// an unknown token (no-op) so callers can defer Release without first
// confirming Reserve succeeded.
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

// MeasureDir returns the on-disk size of an index directory. Reads
// manifest.json (where every file is listed with its size) and falls
// back to a directory walk if the manifest is absent. Used at startup
// to backfill IndexBytes for already-loaded inventories.
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
