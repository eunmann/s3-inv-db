package inventory_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// buildMinimalIndex builds a real, loadable single-object index in a
// tmpdir so a manager can be driven into StateLoaded.
func buildMinimalIndex(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(1, 0)
	agg.AddObject("only/object.bin", 100, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			t.Fatalf("builder.Add: %v", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatalf("builder.Finalize: %v", err)
	}

	return dir
}

// TestManager_Unload_ClearsLoadDuration pins the regression: when a
// run transitions out of StateLoaded, the previous load's duration
// must not survive into the post-unload Info. Without this guard the
// stale value leaks to JSON API endpoints that return Info directly
// (GetInventoryAPI, UnloadInventoryAPI, RegisterInventoryAPI, etc.),
// regardless of the HTML-side state gate.
func TestManager_Unload_ClearsLoadDuration(t *testing.T) {
	mgr, _ := openManagerWithStore(t)
	indexDir := buildMinimalIndex(t)

	const id inventory.ID = "src/inv1/2026-05-13T03-00Z"
	if err := mgr.Hydrate(t.Context(), inventory.Info{
		ID:           id,
		Name:         "src/inv1 @ 2026-05-13T03-00Z",
		Path:         "s3://src/inv1/manifest.json",
		State:        inventory.StateLoaded,
		LoadDuration: 70 * time.Millisecond,
		LoadedAt:     time.Now().Add(-time.Minute),
	}, indexDir); err != nil {
		t.Fatalf("Hydrate into StateLoaded: %v", err)
	}

	pre, ok := mgr.Get(id)
	if !ok || pre.State != inventory.StateLoaded || pre.LoadDuration == 0 {
		t.Fatalf("pre-unload state = %+v, want StateLoaded with LoadDuration > 0", pre)
	}

	if err := mgr.Unload(t.Context(), id); err != nil {
		t.Fatalf("Unload: %v", err)
	}

	got, ok := mgr.Get(id)
	if !ok {
		t.Fatal("Get after Unload returned !ok")
	}
	if got.State != inventory.StateNotLoaded {
		t.Errorf("state = %s, want %s", got.State, inventory.StateNotLoaded)
	}
	if got.LoadDuration != 0 {
		t.Errorf("LoadDuration = %s, want 0 — stale duration leaks to JSON consumers", got.LoadDuration)
	}
}

// errFakeLoadFailed simulates a build failure (err113 forbids inline
// errors.New at call sites).
var errFakeLoadFailed = errors.New("simulated build failure")

// TestManager_LoadFailure_ClearsLoadDuration covers the analogue path:
// a failed re-load after a successful one must also clear the previous
// LoadDuration. The state machine's invariant is "LoadDuration is only
// meaningful when State == StateLoaded"; an Error state inheriting a
// stale 70ms is the same bug class as a NotLoaded state doing so.
func TestManager_LoadFailure_ClearsLoadDuration(t *testing.T) {
	mgr, _ := openManagerWithStore(t)
	indexDir := buildMinimalIndex(t)

	const id inventory.ID = "src/inv1/2026-05-13T03-00Z"
	if err := mgr.Hydrate(t.Context(), inventory.Info{
		ID:           id,
		Name:         "src/inv1 @ 2026-05-13T03-00Z",
		Path:         "s3://src/inv1/manifest.json",
		State:        inventory.StateError,
		Error:        "previous run failed",
		LoadDuration: 70 * time.Millisecond,
	}, indexDir); err != nil {
		t.Fatalf("Hydrate: %v", err)
	}

	err := mgr.LoadWith(t.Context(), id, func(context.Context, inventory.Info) (string, error) {
		return "", errFakeLoadFailed
	})
	if err == nil {
		t.Fatal("LoadWith returned nil, want build error")
	}

	got, ok := mgr.Get(id)
	if !ok {
		t.Fatal("Get after failed LoadWith returned !ok")
	}
	if got.State != inventory.StateError {
		t.Errorf("state = %s, want %s", got.State, inventory.StateError)
	}
	if got.LoadDuration != 0 {
		t.Errorf("LoadDuration = %s, want 0 after a failed re-load", got.LoadDuration)
	}
}
