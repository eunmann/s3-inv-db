package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// ErrNotFound is returned when an inventory is not found.
var ErrNotFound = errors.New("inventory not found")

// ErrAlreadyExists is returned when attempting to register an inventory that already exists.
var ErrAlreadyExists = errors.New("inventory already exists")

// ErrNotLoaded is returned when attempting to query an inventory that is not loaded.
var ErrNotLoaded = errors.New("inventory not loaded")

// ErrInvalidState is returned when attempting an invalid state transition.
var ErrInvalidState = errors.New("invalid state for operation")

// managedInventory wraps an inventory with its index. The per-inventory
// mu protects the index pointer's lifecycle: readers (WithIndex) hold a
// read lock for the duration of their fn so Unload/Remove/Close cannot
// unmap the underlying mmap files mid-read.
type managedInventory struct {
	mu    sync.RWMutex
	info  Info
	index *indexread.Index
}

// Manager manages multiple inventories with thread-safe access.
type Manager struct {
	mu          sync.RWMutex
	inventories map[string]*managedInventory
}

// NewManager creates a new inventory manager.
func NewManager() *Manager {
	return &Manager{
		inventories: make(map[string]*managedInventory),
	}
}

// Register adds a new inventory in pending state.
func (m *Manager) Register(id, name, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.inventories[id]; exists {
		return ErrAlreadyExists
	}

	m.inventories[id] = &managedInventory{
		info: Info{
			ID:    id,
			Name:  name,
			Path:  path,
			State: StatePending,
		},
	}

	return nil
}

// BuildFunc is the contract a Manager uses to materialise an on-disk index
// before opening it. The returned path must be a directory acceptable to
// indexread.Open. Implementations should be safe to call without holding
// the Manager's lock — Load drops it during the build.
type BuildFunc func(ctx context.Context, info Info) (indexDir string, err error)

// openLocalPath is the default BuildFunc: it treats inv.Path as a local
// directory and returns it unmodified.
func openLocalPath(_ context.Context, info Info) (string, error) {
	return info.Path, nil
}

// Load builds (if needed) and opens the inventory index, using the
// default BuildFunc (interpret Path as a local directory). Suitable for
// legacy callers; new code should use LoadWith.
func (m *Manager) Load(ctx context.Context, id string) error {
	return m.LoadWith(ctx, id, openLocalPath)
}

// LoadWith calls build outside the manager lock to materialise an on-disk
// index, then opens it. State transitions are pending|unloaded|error → parsing
// → loaded|error. Races against Remove or another Load are handled by
// re-checking inventory presence after each lock re-acquire.
func (m *Manager) LoadWith(ctx context.Context, id string, build BuildFunc) error {
	m.mu.Lock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.Unlock()
		return ErrNotFound
	}
	if inv.info.State != StatePending && inv.info.State != StateUnloaded && inv.info.State != StateError {
		m.mu.Unlock()
		return fmt.Errorf("%w: cannot load from state %s", ErrInvalidState, inv.info.State)
	}
	inv.info.State = StateParsing
	inv.info.Error = ""
	snapshot := inv.info
	m.mu.Unlock()

	// Materialise the on-disk index outside the lock (can take a while).
	indexDir, buildErr := build(ctx, snapshot)
	var idx *indexread.Index
	var openErr error
	if buildErr == nil {
		idx, openErr = indexread.Open(indexDir)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	inv, exists = m.inventories[id]
	if !exists {
		if idx != nil {
			idx.Close()
		}
		return ErrNotFound
	}

	switch {
	case buildErr != nil:
		inv.info.State = StateError
		inv.info.Error = buildErr.Error()
		return fmt.Errorf("build index: %w", buildErr)
	case openErr != nil:
		inv.info.State = StateError
		inv.info.Error = openErr.Error()
		return fmt.Errorf("open index: %w", openErr)
	case ctx.Err() != nil:
		idx.Close()
		inv.info.State = StateError
		inv.info.Error = ctx.Err().Error()
		return fmt.Errorf("load cancelled: %w", ctx.Err())
	}

	// inv.index assignment is safe under m.mu.Lock — no readers can be
	// inside WithIndex on this inventory because state was StateParsing.
	inv.index = idx
	inv.info.State = StateLoaded
	inv.info.NodeCount = idx.Count()
	inv.info.MaxDepth = idx.MaxDepth()
	inv.info.HasTierData = idx.HasTierData()
	inv.info.LoadedAt = time.Now()
	return nil
}

// Unload closes an inventory index and releases its resources. It blocks
// until any in-flight WithIndex reader on this inventory has returned.
func (m *Manager) Unload(id string) error {
	m.mu.Lock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.Unlock()
		return ErrNotFound
	}
	if inv.info.State != StateLoaded {
		m.mu.Unlock()
		return fmt.Errorf("%w: cannot unload from state %s", ErrInvalidState, inv.info.State)
	}
	inv.info.State = StateUnloaded
	inv.info.NodeCount = 0
	inv.info.MaxDepth = 0
	inv.info.LoadedAt = time.Time{}
	m.mu.Unlock()

	// Close the index outside the Manager lock so other inventories stay
	// available; the per-inventory write lock drains in-flight readers.
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.index != nil {
		inv.index.Close()
		inv.index = nil
	}
	return nil
}

// Get returns info about an inventory.
func (m *Manager) Get(id string) (Info, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inv, exists := m.inventories[id]
	if !exists {
		return Info{}, false
	}

	return inv.info, true
}

// List returns info about all inventories.
func (m *Manager) List() []Info {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Info, 0, len(m.inventories))
	for _, inv := range m.inventories {
		result = append(result, inv.info)
	}

	return result
}

// WithIndex borrows the loaded index for the duration of fn. The index
// is guaranteed to remain open until fn returns; callers must not retain
// the pointer or any slice/string derived from mmap-backed memory beyond
// the call. Concurrent Unload/Remove/Close on the same inventory block
// until fn returns.
func (m *Manager) WithIndex(id string, fn func(*indexread.Index) error) error {
	m.mu.RLock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.RUnlock()
		return ErrNotFound
	}
	if inv.info.State != StateLoaded || inv.index == nil {
		m.mu.RUnlock()
		return ErrNotLoaded
	}
	idx := inv.index
	// Acquire the per-inventory read lock before releasing m.mu so a
	// concurrent Unload/Remove cannot slip in and close idx out from
	// under us. Lock ordering: m.mu → inv.mu (never the reverse).
	inv.mu.RLock()
	m.mu.RUnlock()
	defer inv.mu.RUnlock()

	return fn(idx)
}

// Remove removes an inventory from the manager. It blocks until any
// in-flight WithIndex reader on this inventory has returned.
func (m *Manager) Remove(id string) error {
	m.mu.Lock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.Unlock()
		return ErrNotFound
	}
	delete(m.inventories, id)
	m.mu.Unlock()

	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.index != nil {
		inv.index.Close()
		inv.index = nil
	}
	return nil
}

// Close closes all loaded inventories and clears the manager. It blocks
// until any in-flight WithIndex readers have returned.
func (m *Manager) Close() error {
	m.mu.Lock()
	invs := m.inventories
	m.inventories = make(map[string]*managedInventory)
	m.mu.Unlock()

	var firstErr error
	for _, inv := range invs {
		inv.mu.Lock()
		if inv.index != nil {
			if err := inv.index.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			inv.index = nil
		}
		inv.mu.Unlock()
	}
	return firstErr
}
