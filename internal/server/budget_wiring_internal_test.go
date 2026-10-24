package server

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// newS3Server builds a Server from cfg (DB and CacheDir set by the
// caller) with S3 discovery wired to an offline client and retention 1
// per configuration.
func newS3Server(t *testing.T, cfg Config) *Server {
	t.Helper()
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	cfg.Addr = ":0"
	cfg.Logger = zerolog.Nop()
	cfg.PriceTable = pricing.DefaultUSEast1Prices()
	cfg.S3Source = "s3://inventory-dest/"
	cfg.MaxIndexDisk = 1 << 30
	cfg.AutoLoadRetentionDefault = 1
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.manager.Close() })

	return srv
}

// buildRunCache writes a small index where the loader keeps the cache
// for src/inv/<run> and returns that directory.
func buildRunCache(t *testing.T, cacheRoot, run string) string {
	t.Helper()
	dir := filepath.Join(cacheRoot, "src", "inv", run)
	agg := extsort.NewAggregator(2, 0)
	agg.AddObject("a/x", 10, tiers.Standard)
	agg.AddObject("b/y", 20, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}

	return dir
}

// A budget eviction must delete the evicted run's cache: with the
// server passing no remover, or the wrong cache key, runs were unloaded
// but their caches stayed on disk outside --max-index-disk.
func TestServer_BudgetEvictionRemovesCache(t *testing.T) {
	ctx := t.Context()
	cacheRoot := t.TempDir()
	srv := newS3Server(t, Config{DB: testDB(t), CacheDir: cacheRoot})

	runs := []string{"2026-01-01T00-00Z", "2026-01-02T00-00Z"}
	dirs := make([]string, len(runs))
	for i, run := range runs {
		dirs[i] = buildRunCache(t, cacheRoot, run)
		id := inventory.ID("src/inv/" + run)
		if err := srv.manager.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
		build := func(context.Context, inventory.Info) (string, error) { return dirs[i], nil }
		if err := srv.gate.Load(ctx, id, build, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}

	if info, _ := srv.manager.Get("src/inv/" + inventory.ID(runs[0])); info.State != inventory.StateNotLoaded {
		t.Fatalf("older run state = %s, want not_loaded after retention eviction", info.State)
	}
	if _, err := os.Stat(dirs[0]); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("evicted run's cache %s still present (stat err %v)", dirs[0], err)
	}
	if _, err := os.Stat(dirs[1]); err != nil {
		t.Errorf("loaded run's cache: %v", err)
	}
}

// A loaded index persisted before sizes were recorded must have its
// size measured and persisted at startup; the tracker sums sizes from
// the catalog, so without it the index counted as 0 bytes.
func TestServer_BackfillIndexSizesPersistsMeasuredSize(t *testing.T) {
	ctx := t.Context()
	cacheRoot := t.TempDir()
	const id inventory.ID = "src/inv/2026-01-01T00-00Z"
	dir := buildRunCache(t, cacheRoot, id.Split().Run)
	want, err := inventory.MeasureDir(ctx, dir)
	if err != nil || want == 0 {
		t.Fatalf("MeasureDir = %d, %v", want, err)
	}

	db := testDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := invStore.Upsert(ctx, inventory.Info{ID: id, Name: "n", Path: "s3://inventory-dest/src/inv/manifest.json", State: inventory.StateLoaded}); err != nil {
		t.Fatal(err)
	}
	srv := newS3Server(t, Config{DB: db, CacheDir: cacheRoot})

	if got, _ := srv.manager.Get(id); got.State != inventory.StateLoaded || got.IndexBytes != want {
		t.Errorf("catalog state/size = %s/%d, want loaded/%d", got.State, got.IndexBytes, want)
	}
	if got := srv.tracker.Used(); got != want {
		t.Errorf("tracker used = %d, want %d", got, want)
	}
	persisted, err := invStore.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.IndexBytes != want {
		t.Errorf("persisted size = %d, want %d", persisted.IndexBytes, want)
	}
}
