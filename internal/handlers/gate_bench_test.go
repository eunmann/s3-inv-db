package handlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/go-chi/chi/v5"
)

// BenchmarkGate* here time one browse or compare API request over a
// prefix with gateWideChildren children: the per-page-view cost the
// North star bounds by the page shown, not by the child count. Fixtures
// are built per benchmark, outside the timed loop.

const (
	gateWideChildren = 200_000
	gateFromID       = "src/inv/2026-01-01T00-00Z"
	gateToID         = "src/inv/2026-01-02T00-00Z"
)

// buildWideIndex writes an index whose root has gateWideChildren
// children. Every tenth child differs when changed is true, so a compare
// against the unchanged index has a realistic mix of statuses.
func buildWideIndex(b *testing.B, dir string, changed bool) {
	b.Helper()
	agg := extsort.NewAggregator(gateWideChildren*2, 0)
	for i := range gateWideChildren {
		size := uint64(i%977 + 1)
		if changed && i%10 == 0 {
			size += 13
		}
		tier := tiers.Standard
		if i%3 == 0 {
			tier = tiers.GlacierFR
		}
		agg.AddObject(fmt.Sprintf("c%06d/a", i), size, tier)
		agg.AddObject(fmt.Sprintf("c%06d/b", i), size*2, tiers.Standard)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		b.Fatal(err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			b.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		b.Fatal(err)
	}
}

// wideRouter builds both runs and a router serving the browse and
// compare APIs. Built per benchmark: with b.Loop the setup runs once,
// outside the timed loop.
func wideRouter(b *testing.B) http.Handler {
	b.Helper()
	mgr := inventory.NewCatalog(nil)
	b.Cleanup(func() { _ = mgr.Close() })
	for _, run := range []struct {
		id      inventory.ID
		changed bool
	}{{gateFromID, false}, {gateToID, true}} {
		dir := filepath.Join(b.TempDir(), "index")
		buildWideIndex(b, dir, run.changed)
		if err := mgr.Register(b.Context(), run.id, "n", dir); err != nil {
			b.Fatal(err)
		}
		if err := mgr.Load(b.Context(), run.id); err != nil {
			b.Fatal(err)
		}
	}
	h, _ := newWiredHandlersWithConfigs(b, mgr)
	r := chi.NewRouter()
	r.Get("/api/browse", h.BrowseLevelAPI)
	r.Get("/api/compare", h.CompareLevelAPI)

	return r
}

func benchGet(b *testing.B, router http.Handler, target string) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequestWithContext(b.Context(), http.MethodGet, target, http.NoBody))
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkGateBrowseWideBySize(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/browse?inventory_id="+gateFromID+"&sort=size&dir=desc")
}

func BenchmarkGateBrowseWideByName(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/browse?inventory_id="+gateFromID+"&page=50")
}

func BenchmarkGateCompareWide(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/compare?from="+gateFromID+"&to="+gateToID)
}
