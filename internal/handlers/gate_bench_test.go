package handlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
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
	buildWideIndexKeys(b, dir, changed, "c%06d/a", "c%06d/b")
}

// buildWideIndexKeys is buildWideIndex with each child's two object
// keys formatted from keyA and keyB.
func buildWideIndexKeys(b *testing.B, dir string, changed bool, keyA, keyB string) {
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
		agg.AddObject(fmt.Sprintf(keyA, i), size, tier)
		agg.AddObject(fmt.Sprintf(keyB, i), size*2, tiers.Standard)
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
// compare APIs, without the level cache: every request is a level's
// first view. Built per benchmark: with b.Loop the setup runs once,
// outside the timed loop.
func wideRouter(b *testing.B) http.Handler {
	b.Helper()

	return apiRouter(wideHandlersWith(b, buildWideIndex, handlers.WithLevelCacheBytes(0)))
}

func apiRouter(h *handlers.Handlers) http.Handler {
	r := chi.NewRouter()
	r.Get("/api/browse", h.BrowseLevelAPI)
	r.Get("/api/compare", h.CompareLevelAPI)

	return r
}

// pagedRouter is the router the Paged entries page through, with the
// default level cache.
func pagedRouter(b *testing.B) http.Handler {
	b.Helper()

	return apiRouter(wideHandlers(b))
}

// wideHandlers builds both runs and returns handlers serving them.
func wideHandlers(b *testing.B) *handlers.Handlers {
	b.Helper()

	return wideHandlersWith(b, buildWideIndex)
}

// wideHandlersWith is wideHandlers with each run built by build.
func wideHandlersWith(b *testing.B, build func(b *testing.B, dir string, changed bool), opts ...handlers.Option) *handlers.Handlers {
	b.Helper()
	mgr := inventory.NewCatalog(nil)
	b.Cleanup(func() { _ = mgr.Close() })
	for _, run := range []struct {
		id      inventory.ID
		changed bool
	}{{gateFromID, false}, {gateToID, true}} {
		dir := filepath.Join(b.TempDir(), "index")
		build(b, dir, run.changed)
		if err := mgr.Register(b.Context(), run.id, "n", dir); err != nil {
			b.Fatal(err)
		}
		if err := mgr.Load(b.Context(), run.id); err != nil {
			b.Fatal(err)
		}
	}
	h, _ := newWiredHandlersWithConfigs(b, mgr, opts...)

	return h
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
	benchGet(b, wideRouter(b), "/api/browse?inventory_id="+gateFromID+"&sort=segment&page=50")
}

func BenchmarkGateCompareWide(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/compare?from="+gateFromID+"&to="+gateToID)
}

func BenchmarkGateBrowseWideByCost(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/browse?inventory_id="+gateFromID+"&sort=cost&dir=desc")
}

func BenchmarkGateCompareWideByCost(b *testing.B) {
	benchGet(b, wideRouter(b), "/api/compare?from="+gateFromID+"&to="+gateToID+"&sort=cost&dir=desc")
}

// BenchmarkGate*Paged: a user paging through one wide level on one
// server: each iteration views page 1, then a later page, of the same
// order. The other gate entries time a level's first view.
func benchPaged(b *testing.B, router http.Handler, targets ...string) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		for _, target := range targets {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequestWithContext(b.Context(), http.MethodGet, target, http.NoBody))
			if rec.Code != http.StatusOK {
				b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
		}
	}
}

func BenchmarkGateBrowseWideBySizePaged(b *testing.B) {
	const q = "/api/browse?inventory_id=" + gateFromID + "&sort=size&dir=desc"
	benchPaged(b, pagedRouter(b), q, q+"&page=50")
}

func BenchmarkGateBrowseWideByCostPaged(b *testing.B) {
	const q = "/api/browse?inventory_id=" + gateFromID + "&sort=cost&dir=desc"
	benchPaged(b, pagedRouter(b), q, q+"&page=50")
}

func BenchmarkGateCompareWidePaged(b *testing.B) {
	const q = "/api/compare?from=" + gateFromID + "&to=" + gateToID
	benchPaged(b, pagedRouter(b), q, q+"&page=50")
}
