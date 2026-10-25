package inventory

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// buildServedIndex builds a loadable single-object index and returns
// its dir and manifest.
func buildServedIndex(t *testing.T) (string, *format.Manifest) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(1, 0)
	agg.AddObject("only/object.bin", 100, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatal(err)
	}
	m, err := format.ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}

	return dir, m
}

func prefaulted(c *Catalog, id ID) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.inventories[id].prefaultBytes
}

// The prefault share bounds the indexes the catalog prefaults together,
// so a restart with many loaded indexes reads at most that much.
func TestPrefaultBudgetSharedAcrossCatalog(t *testing.T) {
	dirA, m := buildServedIndex(t)
	dirB, _ := buildServedIndex(t)
	dirC, _ := buildServedIndex(t)
	size := m.TotalBytes()
	c := NewCatalog(nil)
	t.Cleanup(func() { _ = c.Close() })
	c.prefaultLimit = 2 * size

	for id, dir := range map[ID]string{"a": dirA, "b": dirB} {
		if err := c.Hydrate(t.Context(), Info{ID: id, State: StateLoaded}, dir); err != nil {
			t.Fatal(err)
		}
		if got := prefaulted(c, id); got != size {
			t.Errorf("hydrate %s: prefaulted %d bytes, want %d", id, got, size)
		}
	}
	if err := c.Register(t.Context(), "c", "c", dirC); err != nil {
		t.Fatal(err)
	}
	build := func(context.Context, Info) (string, error) { return dirC, nil }
	if err := c.LoadWith(t.Context(), "c", build); err != nil {
		t.Fatal(err)
	}
	if got := prefaulted(c, "c"); got != 0 {
		t.Errorf("load past the budget prefaulted %d bytes, want 0", got)
	}

	if err := c.Unload(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Unload(t.Context(), "c"); err != nil {
		t.Fatal(err)
	}
	if err := c.LoadWith(t.Context(), "c", build); err != nil {
		t.Fatal(err)
	}
	if got := prefaulted(c, "c"); got != size {
		t.Errorf("load after an unload freed the budget prefaulted %d bytes, want %d", got, size)
	}
}

func TestPlanPrefaultWithoutManifest(t *testing.T) {
	c := NewCatalog(nil)
	c.prefaultLimit = 1 << 40
	inv := &managedInventory{info: Info{ID: "a", State: StateLoading}}
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.planPrefaultLocked(inv, t.TempDir()); m != nil || inv.prefaultBytes != 0 {
		t.Errorf("planned a prefault of %d bytes for an index with no manifest", inv.prefaultBytes)
	}
}

// Prefaulting only warms the page cache; an index it fails on still
// opens.
func TestOpenServedSurvivesPrefaultError(t *testing.T) {
	dir, m := buildServedIndex(t)
	m.Files["missing.bin"] = format.FileInfo{Size: 1}
	idx, err := openServed(dir, m)
	if err != nil {
		t.Fatalf("openServed failed on a prefault error: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}
