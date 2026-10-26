//go:build integration

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// TestIndexSource_DiscoversDownloadsAndServesAPublishedRun: a server
// with only an index source finds a published run after a refresh,
// loads it by download through the normal (budgeted) load path, and
// answers queries from it.
func TestIndexSource_DiscoversDownloadsAndServesAPublishedRun(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	run := indexstore.Run{Source: "src-bucket", Inventory: "daily", Run: "2026-10-24T01-00Z"}
	if err := indexstore.New(raw, bucket).Publish(t.Context(), smallIndex(t), run.Prefix("idx/")); err != nil {
		t.Fatal(err)
	}

	srv, err := New(t.Context(), Config{
		Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t),
		CacheDir: t.TempDir(), IndexSource: "s3://" + bucket + "/idx/", MaxIndexDisk: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.manager.Close() })

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/api/indexes/refresh", http.NoBody)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"auto_load":false`) {
		t.Fatalf("refresh: %d %s", w.Code, w.Body.String())
	}
	views, _, err := srv.discovery.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var target *inventory.MergedInventory
	for i := range views {
		if views[i].Run == run.Run {
			target = &views[i]
		}
	}
	if target == nil || target.FileFormat != "index" {
		t.Fatalf("snapshot %+v lacks the published run as an index", views)
	}
	if err := srv.discovery.Load(t.Context(), target.Inventory); err != nil {
		t.Fatalf("load: %v", err)
	}
	err = srv.manager.WithIndex(target.CompositeID(), func(idx *indexread.Index) error {
		if s, ok := idx.StatsForPrefix("data/"); !ok || s.ObjectCount != 500 {
			t.Errorf("data/ = %+v (%v), want 500 objects", s, ok)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func smallIndex(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(500, 0)
	for i := range 500 {
		agg.AddObject(fmt.Sprintf("data/%02d/f%d", i%20, i), uint64(i+1), tiers.Standard)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
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
