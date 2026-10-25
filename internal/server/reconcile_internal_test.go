package server

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// restoreFixture is a server over a cache root holding the indexes of
// two persisted runs: one loaded, one not.
type restoreFixture struct {
	srv       *Server
	db        *sql.DB
	loadedDir string
	staleDir  string
}

const (
	loadedRunID inventory.ID = "src/inv/loaded"
	staleRunID  inventory.ID = "src/inv/stale"
)

func newRestoreFixture(t *testing.T) restoreFixture {
	t.Helper()
	db := testDB(t)
	stores, err := newStores(Config{DB: db, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("newStores: %v", err)
	}
	bldr := loader.New(t.TempDir(), nil)
	f := restoreFixture{
		db: db,
		srv: &Server{
			manager:  stores.catalog,
			invStore: stores.inv,
			jobStore: stores.jobStore,
			bldr:     bldr,
		},
	}
	t.Cleanup(func() { _ = stores.catalog.Close() })
	for _, run := range []struct {
		id    inventory.ID
		dir   *string
		state inventory.State
	}{
		{loadedRunID, &f.loadedDir, inventory.StateLoaded},
		{staleRunID, &f.staleDir, inventory.StateNotLoaded},
	} {
		p := run.id.Split()
		dir, err := bldr.CacheDirFor(inventory.CacheKey{SourceBucket: p.Source, InventoryID: p.Inventory, Run: p.Run})
		if err != nil {
			t.Fatalf("CacheDirFor: %v", err)
		}
		buildIndexAt(t, dir)
		*run.dir = dir
		if err := stores.inv.Upsert(t.Context(), inventory.Info{ID: run.id, Name: string(run.id), Path: "s3://dst/manifest.json", State: run.state}); err != nil {
			t.Fatalf("seed %s: %v", run.id, err)
		}
	}

	return f
}

// buildIndexAt writes a minimal real index to dir, so it both opens and
// reads as one of this project's indexes.
func buildIndexAt(t *testing.T, dir string) {
	t.Helper()
	agg := extsort.NewAggregator(1, 0)
	agg.AddObject("a/x", 1, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
}

func dirExists(path string) bool {
	_, err := os.Stat(filepath.Join(path, format.ManifestFile))

	return err == nil
}

// TestRestore_ReconcileKeepsOnlyLoadedRuns: at startup the loaded run's
// index is hydrated and kept; the index of a run that isn't loaded is
// removed.
func TestRestore_ReconcileKeepsOnlyLoadedRuns(t *testing.T) {
	f := newRestoreFixture(t)
	f.srv.restore(t.Context(), zerolog.Nop())

	if got, _ := f.srv.manager.Get(loadedRunID); got.State != inventory.StateLoaded {
		t.Errorf("loaded run state = %s (%s), want loaded", got.State, got.Error)
	}
	if !dirExists(f.loadedDir) {
		t.Error("the loaded run's index was removed")
	}
	if dirExists(f.staleDir) {
		t.Error("the index of a run that isn't loaded survived")
	}
}

// TestRestore_SkipsReconcileWhenCatalogNotRestored: when the store
// can't be read the catalog is empty, and reconciling against it would
// delete every loaded run's index. Nothing is removed.
func TestRestore_SkipsReconcileWhenCatalogNotRestored(t *testing.T) {
	f := newRestoreFixture(t)
	if err := f.db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	f.srv.restore(t.Context(), zerolog.Nop())

	for _, dir := range []string{f.loadedDir, f.staleDir} {
		if !dirExists(dir) {
			t.Errorf("%s was removed although the catalog wasn't restored", dir)
		}
	}
}
