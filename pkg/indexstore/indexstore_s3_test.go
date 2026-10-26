//go:build integration

package indexstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// buildIndex writes a small multi-tier index to a new directory.
func buildIndex(t *testing.T, objects int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(objects, 0)
	for i := range objects {
		tier := tiers.Standard
		if i%3 == 0 {
			tier = tiers.GlacierFR
		}
		agg.AddObject(fmt.Sprintf("logs/%03d/day%02d/f%d", i%40, i%7, i), uint64(i+1), tier)
	}
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

func publish(t *testing.T, objects int) (*indexstore.Store, string, string) { //nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
	t.Helper()
	c := s3test.RawClient(t)
	store := indexstore.New(c, s3test.Bucket(t, c))
	dir := buildIndex(t, objects)
	prefix := indexstore.Run{Source: "src-bucket", Inventory: "daily", Run: "2026-10-24T01-00Z"}.Prefix("indexes")
	if err := store.Publish(t.Context(), dir, prefix); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	return store, dir, prefix
}

func TestPublishFetchRoundTrip(t *testing.T) {
	store, src, prefix := publish(t, 3000)
	m, err := store.ReadManifest(t.Context(), prefix)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "fetched")
	var last uint64
	if err := store.Fetch(t.Context(), prefix, dst, func(done, _ uint64) { last = done }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if last != m.TotalBytes() {
		t.Errorf("progress ended at %d bytes, want %d", last, m.TotalBytes())
	}
	a, err := indexread.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := indexread.Open(dst)
	if err != nil {
		t.Fatalf("open fetched index: %v", err)
	}
	defer b.Close()
	if a.Count() != b.Count() {
		t.Fatalf("fetched index has %d prefixes, want %d", b.Count(), a.Count())
	}
	for _, p := range []string{"", "logs/", "logs/007/", "logs/007/day03/"} {
		sa, okA := a.StatsForPrefix(p)
		sb, okB := b.StatsForPrefix(p)
		if okA != okB || sa != sb {
			t.Errorf("%q: fetched %+v (%v), want %+v (%v)", p, sb, okB, sa, okA)
		}
	}
}

// TestFetchRefusesAnUncommittedRun: an upload interrupted before the
// manifest leaves files but no manifest; the run reads as unpublished
// and nothing is fetched.
func TestFetchRefusesAnUncommittedRun(t *testing.T) {
	store, _, prefix := publish(t, 500)
	deleteKey(t, store.Bucket(), prefix+format.ManifestFile)
	if _, err := store.ReadManifest(t.Context(), prefix); !errors.Is(err, indexstore.ErrNotPublished) {
		t.Fatalf("ReadManifest err = %v, want ErrNotPublished", err)
	}
	dst := filepath.Join(t.TempDir(), "fetched")
	if err := store.Fetch(t.Context(), prefix, dst, nil); !errors.Is(err, indexstore.ErrNotPublished) {
		t.Fatalf("Fetch err = %v, want ErrNotPublished", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Errorf("Fetch of an uncommitted run left %s", dst)
	}
}

// TestFetchRejectsATamperedFile: a file whose bytes don't match the
// manifest fails verification, and the fetch leaves nothing behind.
func TestFetchRejectsATamperedFile(t *testing.T) {
	store, src, prefix := publish(t, 500)
	info, err := os.Stat(filepath.Join(src, format.CoreStatsFile))
	if err != nil {
		t.Fatal(err)
	}
	putKey(t, store.Bucket(), prefix+format.CoreStatsFile, strings.Repeat("x", int(info.Size())))
	dst := filepath.Join(t.TempDir(), "fetched")
	err = store.Fetch(t.Context(), prefix, dst, nil)
	if !errors.Is(err, format.ErrChecksumMismatch) {
		t.Fatalf("Fetch err = %v, want ErrChecksumMismatch", err)
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Errorf("failed Fetch left %s", dst)
	}
}

// TestRepublishReplacesTheRun: publishing over a run replaces it; the
// fetch afterwards gets the new index.
func TestRepublishReplacesTheRun(t *testing.T) {
	store, _, prefix := publish(t, 500)
	bigger := buildIndex(t, 2000)
	if err := store.Publish(t.Context(), bigger, prefix); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "fetched")
	if err := store.Fetch(t.Context(), prefix, dst, nil); err != nil {
		t.Fatal(err)
	}
	want, err := format.ReadManifest(bigger)
	if err != nil {
		t.Fatal(err)
	}
	got, err := format.ReadManifest(dst)
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeCount != want.NodeCount {
		t.Errorf("fetched %d prefixes, want the republished %d", got.NodeCount, want.NodeCount)
	}
}

func deleteKey(t *testing.T, bucket, key string) {
	t.Helper()
	if _, err := s3test.RawClient(t).DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
}

func putKey(t *testing.T, bucket, key, body string) {
	t.Helper()
	if _, err := s3test.RawClient(t).PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body),
	}); err != nil {
		t.Fatal(err)
	}
}
