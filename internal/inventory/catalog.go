package inventory

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
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
	info             Info
	index            *indexread.Index
	lastAccessedNano atomic.Int64
	mu               sync.RWMutex
}

// mirrorStore is the persistence sink Catalog mirrors state transitions
// to. Defaulted to a no-op so callers that don't need durable storage
// (tests, transient operations) can use Catalog directly without nil
// checks at every mirror site.
type mirrorStore interface {
	Upsert(ctx context.Context, info Info) error
	Delete(ctx context.Context, id ID) error
}

type noopMirrorStore struct{}

func (noopMirrorStore) Upsert(context.Context, Info) error { return nil }
func (noopMirrorStore) Delete(context.Context, ID) error   { return nil }

// Catalog manages multiple inventories with thread-safe access.
// Persistence is mirrored to a Store when one is attached via SetStore;
// otherwise mirror calls are silently dropped.
type Catalog struct {
	inventories map[ID]*managedInventory
	store       mirrorStore
	mu          sync.RWMutex
}

// NewCatalog wires a persistence sink. A nil store is replaced with a
// no-op so tests don't need a SQLite handle. After construction every
// state transition (Register, Load, Unload, Remove, Hydrate) is
// mirrored to the store.
func NewCatalog(store *Store) *Catalog {
	c := &Catalog{
		inventories: make(map[ID]*managedInventory),
		store:       noopMirrorStore{},
	}
	if store != nil {
		c.store = store
	}

	return c
}

// SetStore swaps the persistence sink. Test-fixture pattern: a Catalog
// is constructed nil-store, then late-wired to an invStore that's
// shared with peer dependencies (jobs, configStore). Production wires
// via the ctor instead. Safe to call once before first state-changing
// method.
func (m *Catalog) SetStore(s *Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s == nil {
		m.store = noopMirrorStore{}

		return
	}
	m.store = s
}

func (m *Catalog) mirror(ctx context.Context, info Info) error {
	if err := m.store.Upsert(ctx, info); err != nil {
		return fmt.Errorf("mirror to store: %w", err)
	}

	return nil
}

func (m *Catalog) mirrorDelete(ctx context.Context, id ID) error {
	if err := m.store.Delete(ctx, id); err != nil && !errors.Is(err, ErrStoreNotFound) {
		return fmt.Errorf("mirror delete: %w", err)
	}

	return nil
}

// Register adds a new inventory in pending state.
func (m *Catalog) Register(ctx context.Context, id ID, name, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.inventories[id]; exists {
		return ErrAlreadyExists
	}

	info := Info{ID: id, Name: name, Path: path, State: StateNotLoaded}
	m.inventories[id] = &managedInventory{info: info}

	return m.mirror(ctx, info)
}

// BuildFunc is the contract a Catalog uses to materialise an on-disk index
// before opening it. The returned path must be a directory acceptable to
// indexread.Open. Implementations should be safe to call without holding
// the Catalog's lock — Load drops it during the build.
type BuildFunc func(ctx context.Context, info Info) (indexDir string, err error)

// openLocalPath is the default BuildFunc: it treats inv.Path as a local
// directory and returns it unmodified.
func openLocalPath(_ context.Context, info Info) (string, error) {
	return info.Path, nil
}

// Load builds (if needed) and opens the inventory index, using the
// default BuildFunc (interpret Path as a local directory). Marks the
// run as Pinned so it's protected from auto-eviction.
func (m *Catalog) Load(ctx context.Context, id ID) error {
	return m.LoadWith(ctx, id, openLocalPath)
}

// LoadWith calls build outside the manager lock to materialise an
// on-disk index, then opens it. Marks the run as Pinned (manual intent)
// and clears any prior UserUnloadedAt sentinel.
func (m *Catalog) LoadWith(ctx context.Context, id ID, build BuildFunc) error {
	return m.loadInternal(ctx, id, build, true /* pin */)
}

// AutoLoad performs a load on behalf of the background auto-loader.
// Identical to LoadWith except the inventory is not pinned, so future
// auto-eviction may unload it when retention or budget demands.
func (m *Catalog) AutoLoad(ctx context.Context, id ID, build BuildFunc) error {
	return m.loadInternal(ctx, id, build, false /* pin */)
}

func (m *Catalog) loadInternal(ctx context.Context, id ID, build BuildFunc, pin bool) error {
	m.mu.Lock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.Unlock()

		return ErrNotFound
	}
	if !inv.info.State.CanLoad() {
		m.mu.Unlock()

		return fmt.Errorf("%w: cannot load from state %s", ErrInvalidState, inv.info.State)
	}
	inv.info.State = StateLoading
	inv.info.Error = ""
	// LoadDuration belongs to the most recent successful load. Clear it
	// on entry to StateLoading so a build that fails leaves the field
	// at zero rather than carrying a previous run's value through Error
	// state into JSON API responses.
	inv.info.LoadDuration = 0
	if pin {
		inv.info.Pinned = true
		inv.info.UserUnloadedAt = time.Time{}
	}
	loadStartedAt := time.Now()
	_ = m.mirror(ctx, inv.info)
	snapshot := inv.info
	m.mu.Unlock()

	// Materialise the on-disk index outside the lock (can take a while).
	indexDir, buildErr := build(ctx, snapshot)
	var idx *indexread.Index
	var openErr error
	if buildErr == nil {
		idx, openErr = indexread.Open(indexDir)
	}

	var bytes uint64
	if openErr == nil && buildErr == nil && ctx.Err() == nil {
		bytes, _ = measureIndexDir(indexDir)
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
		// Use a fresh ctx for mirror — ctx may be cancelled, but we
		// still need to persist the failure state.
		_ = m.mirror(context.WithoutCancel(ctx), inv.info)

		return fmt.Errorf("build index: %w", buildErr)
	case openErr != nil:
		inv.info.State = StateError
		inv.info.Error = openErr.Error()
		_ = m.mirror(context.WithoutCancel(ctx), inv.info)

		return fmt.Errorf("open index: %w", openErr)
	case ctx.Err() != nil:
		idx.Close()
		inv.info.State = StateError
		inv.info.Error = ctx.Err().Error()
		_ = m.mirror(context.WithoutCancel(ctx), inv.info)

		return fmt.Errorf("load cancelled: %w", ctx.Err())
	}

	// inv.index assignment is safe under m.mu.Lock — no readers can be
	// inside WithIndex on this inventory because state was StateLoading.
	inv.index = idx
	inv.info.State = StateLoaded
	inv.info.NodeCount = idx.Count()
	inv.info.MaxDepth = idx.MaxDepth()
	inv.info.HasTierData = idx.HasTierData()
	inv.info.LoadedAt = time.Now()
	inv.info.LastAccessedAt = inv.info.LoadedAt
	inv.lastAccessedNano.Store(inv.info.LoadedAt.UnixNano())
	inv.info.IndexBytes = bytes
	inv.info.AutoLoadFailureCount = 0
	inv.info.AutoLoadBackoffUntil = time.Time{}
	inv.info.LoadDuration = inv.info.LoadedAt.Sub(loadStartedAt)
	_ = m.mirror(ctx, inv.info)

	return nil
}

// measureIndexDir returns the cumulative byte size of an index by
// reading manifest.json — the build process records each file's size
// there, so we avoid a directory walk. Falls back to walking when the
// manifest is missing or unreadable (covers indexes built by older
// code that didn't list every file).
func measureIndexDir(dir string) (uint64, error) {
	if dir == "" {
		return 0, nil
	}
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("stat %s: %w", dir, err)
	}
	if manifest, err := format.ReadManifest(dir); err == nil && len(manifest.Files) > 0 {
		return manifest.TotalBytes(), nil
	}
	var total uint64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk entry: %w", err)
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
		return 0, fmt.Errorf("walk %s: %w", dir, err)
	}

	return total, nil
}

// Unload closes an inventory index and releases its resources. Used
// for explicit user-driven unloads: stamps UserUnloadedAt so the
// auto-loader treats it as deliberately removed, and clears the pin.
// It blocks until any in-flight WithIndex reader on this inventory has
// returned.
func (m *Catalog) Unload(ctx context.Context, id ID) error {
	return m.unloadInternal(ctx, id, true /* userInitiated */)
}

// EvictForBudget closes an inventory index released by the auto-loader's
// eviction planner. Distinct from Unload because it does NOT stamp
// UserUnloadedAt — the auto-loader is free to reload this run later if
// a newer (or the same) run is discovered and there's budget.
func (m *Catalog) EvictForBudget(ctx context.Context, id ID) error {
	return m.unloadInternal(ctx, id, false /* userInitiated */)
}

func (m *Catalog) unloadInternal(ctx context.Context, id ID, userInitiated bool) error {
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
	inv.info.State = StateNotLoaded
	inv.info.NodeCount = 0
	inv.info.MaxDepth = 0
	inv.info.LoadedAt = time.Time{}
	inv.info.LoadDuration = 0
	inv.info.IndexBytes = 0
	if userInitiated {
		inv.info.UserUnloadedAt = time.Now()
		inv.info.Pinned = false
	}
	_ = m.mirror(ctx, inv.info)
	m.mu.Unlock()

	// Close the index outside the Catalog lock so other inventories stay
	// available; the per-inventory write lock drains in-flight readers.
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.index != nil {
		inv.index.Close()
		inv.index = nil
	}

	return nil
}

// SetPinned flips the pin state of a managed inventory. Returns
// ErrNotFound when id is unknown. Mirrors to the store.
func (m *Catalog) SetPinned(ctx context.Context, id ID, pinned bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	inv, exists := m.inventories[id]
	if !exists {
		return ErrNotFound
	}
	inv.info.Pinned = pinned

	return m.mirror(ctx, inv.info)
}

// RecordAutoLoadFailure marks an auto-load attempt as failed, stamping
// both when it happened (failedAt) and when the next attempt is eligible
// (retryAt). FailedAt is what notification ordering sorts by; retryAt
// gates the next auto-load attempt. Pass time.Time{} for failedAt if the
// caller doesn't have a meaningful timestamp (legacy callers).
func (m *Catalog) RecordAutoLoadFailure(ctx context.Context, id ID, errStr string, failedAt, retryAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	inv, exists := m.inventories[id]
	if !exists {
		return ErrNotFound
	}
	inv.info.AutoLoadFailureCount++
	inv.info.AutoLoadBackoffUntil = retryAt
	inv.info.LastAutoLoadFailedAt = failedAt
	inv.info.Error = errStr

	return m.mirror(ctx, inv.info)
}

// touchedInfo returns inv.info with LastAccessedAt overlaid from the
// atomic. Callers must hold either m.mu or inv.mu while reading the
// rest of inv.info; this helper just patches in the atomic-tracked
// access timestamp.
func (m *Catalog) touchedInfo(inv *managedInventory) Info {
	out := inv.info
	if nano := inv.lastAccessedNano.Load(); nano > 0 {
		out.LastAccessedAt = time.Unix(0, nano)
	}

	return out
}

// TouchAccessed updates the in-memory LastAccessedAt used as the LRU
// tiebreak in eviction. Not persisted — restart resets every entry's
// access time, which is fine.
func (m *Catalog) TouchAccessed(id ID) {
	// Only need a read lock to look up the inventory; the actual
	// timestamp update is an atomic store. This keeps the read hot
	// path (every WithIndex completion calls TouchAccessed) free of
	// the manager-wide write lock that previously serialised every
	// concurrent reader.
	m.mu.RLock()
	inv, ok := m.inventories[id]
	m.mu.RUnlock()
	if ok {
		inv.lastAccessedNano.Store(time.Now().UnixNano())
	}
}

// Get returns info about an inventory.
func (m *Catalog) Get(id ID) (Info, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inv, exists := m.inventories[id]
	if !exists {
		return Info{}, false
	}

	return m.touchedInfo(inv), true
}

// List returns info about all inventories.
func (m *Catalog) List() []Info {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Info, 0, len(m.inventories))
	for _, inv := range m.inventories {
		result = append(result, m.touchedInfo(inv))
	}

	return result
}

// Hydrate adds an inventory back to the manager with state and metadata
// previously persisted to a Store. If info.State is StateLoaded, the
// on-disk index at indexDir is opened; an Open failure flips the
// in-memory state to StateError but the inventory is still registered
// so the UI can show it (and the user can retry). For non-loaded
// states, indexDir is ignored.
func (m *Catalog) Hydrate(ctx context.Context, info Info, indexDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.inventories[info.ID]; exists {
		return ErrAlreadyExists
	}
	mi := &managedInventory{info: info}
	if info.State == StateLoaded {
		if indexDir == "" {
			mi.info.State = StateError
			mi.info.Error = "cannot rehydrate loaded inventory without index dir"
			mi.info.NodeCount = 0
			mi.info.MaxDepth = 0
			mi.info.HasTierData = false
		} else {
			idx, err := indexread.Open(indexDir)
			switch {
			case err != nil:
				mi.info.State = StateError
				mi.info.Error = err.Error()
				mi.info.NodeCount = 0
				mi.info.MaxDepth = 0
				mi.info.HasTierData = false
			default:
				mi.index = idx
				mi.info.NodeCount = idx.Count()
				mi.info.MaxDepth = idx.MaxDepth()
				mi.info.HasTierData = idx.HasTierData()
			}
		}
	}
	m.inventories[info.ID] = mi
	_ = m.mirror(ctx, mi.info)

	return nil
}

// WithIndex borrows the loaded index for the duration of fn. The index
// is guaranteed to remain open until fn returns; callers must not retain
// the pointer or any slice/string derived from mmap-backed memory beyond
// the call. Concurrent Unload/Remove/Close on the same inventory block
// until fn returns.
func (m *Catalog) WithIndex(id ID, fn func(*indexread.Index) error) error {
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
	defer m.TouchAccessed(id)

	return fn(idx)
}

// WithTwoIndexes borrows two loaded indexes for the duration of fn —
// the read-side primitive for the Compare feature. Per-inventory
// locks are acquired in deterministic ID order so two callers comparing
// the same pair in opposite directions cannot deadlock. When idA == idB
// only one lock is taken and the same index pointer is passed in both
// positions, letting callers treat self-compare as a degenerate case.
func (m *Catalog) WithTwoIndexes(idA, idB ID, fn func(a, b *indexread.Index) error) error {
	m.mu.RLock()
	invA, okA := m.inventories[idA]
	invB, okB := m.inventories[idB]
	if !okA || !okB {
		m.mu.RUnlock()

		return ErrNotFound
	}
	if invA.info.State != StateLoaded || invA.index == nil ||
		invB.info.State != StateLoaded || invB.index == nil {
		m.mu.RUnlock()

		return ErrNotLoaded
	}
	idxA, idxB := invA.index, invB.index

	if idA == idB {
		invA.mu.RLock()
		m.mu.RUnlock()
		defer invA.mu.RUnlock()
		defer m.TouchAccessed(idA)

		return fn(idxA, idxA)
	}

	first, second := invA, invB
	if idA > idB {
		first, second = invB, invA
	}
	first.mu.RLock()
	second.mu.RLock()
	m.mu.RUnlock()
	defer second.mu.RUnlock()
	defer first.mu.RUnlock()
	defer func() {
		m.TouchAccessed(idA)
		m.TouchAccessed(idB)
	}()

	return fn(idxA, idxB)
}

// Remove removes an inventory from the manager. It blocks until any
// in-flight WithIndex reader on this inventory has returned.
func (m *Catalog) Remove(ctx context.Context, id ID) error {
	m.mu.Lock()
	inv, exists := m.inventories[id]
	if !exists {
		m.mu.Unlock()

		return ErrNotFound
	}
	delete(m.inventories, id)
	_ = m.mirrorDelete(ctx, id)
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
func (m *Catalog) Close() error {
	m.mu.Lock()
	invs := m.inventories
	m.inventories = make(map[ID]*managedInventory)
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
