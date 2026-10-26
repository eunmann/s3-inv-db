//go:build integration

package loader_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func builtIndex(t *testing.T) string {
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

// TestBuildWith_FetchesAPublishedIndex: with an index source, a run a
// build published is downloaded, verified and moved into the cache; a
// run that isn't published fails without building when the loader may
// not build, and leaves nothing behind.
func TestBuildWith_FetchesAPublishedIndex(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	store := indexstore.New(raw, bucket)
	key := inventory.CacheKey{SourceBucket: "src-bucket", InventoryID: "daily", Run: "2026-10-24T01-00Z"}
	prefix := indexstore.Run{Source: key.SourceBucket, Inventory: key.InventoryID, Run: key.Run}.Prefix("indexes/")
	if err := store.Publish(t.Context(), builtIndex(t), prefix); err != nil {
		t.Fatal(err)
	}

	cache := t.TempDir()
	l := loader.New(cache, s3test.FetchClient(t), loader.WithIndexSource(store, "indexes/", false))
	manifestURI := "s3://" + bucket + "/" + prefix + "manifest.json"
	var stages []string
	out, err := l.BuildWith(t.Context(), key, manifestURI, func(stage string, _, _ int64) { stages = append(stages, stage) }, nil)
	if err != nil {
		t.Fatalf("BuildWith: %v", err)
	}
	idx, err := indexread.Open(out)
	if err != nil {
		t.Fatalf("open fetched index: %v", err)
	}
	if s, ok := idx.StatsForPrefix("data/"); !ok || s.ObjectCount != 500 {
		t.Errorf("data/ = %+v (%v), want 500 objects", s, ok)
	}
	_ = idx.Close()
	if len(stages) < 2 || stages[1] != "downloading" {
		t.Errorf("stages = %v, want preparing then downloading", stages)
	}

	missing := inventory.CacheKey{SourceBucket: "src-bucket", InventoryID: "daily", Run: "2026-10-25T01-00Z"}
	if _, err := l.BuildWith(t.Context(), missing, manifestURI, nil, nil); !errors.Is(err, indexstore.ErrNotPublished) {
		t.Fatalf("unpublished run: err = %v, want ErrNotPublished", err)
	}
	entries, err := os.ReadDir(filepath.Join(cache, "src-bucket", "daily"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != key.Run {
		t.Errorf("cache holds %v, want only the fetched run", entries)
	}
}

// TestBuildWith_BuildsWhenNotPublished: a loader that may build falls
// back to the in-process build for a run with no published index.
func TestBuildWith_BuildsWhenNotPublished(t *testing.T) {
	raw := s3test.RawClient(t)
	l := loader.New(t.TempDir(), s3test.FetchClient(t), loader.WithIndexSource(indexstore.New(raw, s3test.Bucket(t, raw)), "indexes/", true))
	_, err := l.BuildWith(t.Context(), inventory.CacheKey{SourceBucket: "src-bucket", InventoryID: "daily", Run: "2026-10-25T01-00Z"},
		"s3://no-such-bucket-s3invtest/inv/manifest.json", nil, nil)
	if err == nil || errors.Is(err, indexstore.ErrNotPublished) {
		t.Fatalf("err = %v, want the in-process build's error (it ran)", err)
	}
}
