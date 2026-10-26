//go:build integration

package s3disco_test

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog"
)

func TestDiscoverer_List_AgainstLocalS3(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)

	// Upload two inventories with two timestamp folders each, via the
	// seeder package so we exercise the same code path the CLI uses.
	now := time.Now().UTC()
	earlier := now.Add(-time.Hour)
	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"

	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", earlier, 100, 1001)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", now, 100, 1002)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-002", now, 100, 2001)

	d := s3disco.New(client, bucket, prefix)
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Discovery now returns one entry PER RUN: inv-001 has two runs,
	// inv-002 has one — total three.
	if len(got) != 3 {
		t.Fatalf("len(List) = %d, want 3 (2 runs of inv-001 + 1 of inv-002); got=%+v", len(got), got)
	}

	nowStamp := now.Format("2006-01-02T15-04Z")
	earlierStamp := earlier.Format("2006-01-02T15-04Z")

	// Group by (src, inv).
	runsByInv := map[string][]string{}
	for _, e := range got {
		if e.SourceBucket != srcBucket {
			t.Errorf("entry has SourceBucket = %q, want %q", e.SourceBucket, srcBucket)
		}
		runsByInv[e.Name] = append(runsByInv[e.Name], e.Run)
	}

	if got := runsByInv["inv-001"]; len(got) != 2 {
		t.Errorf("inv-001 runs = %v, want 2 entries", got)
	} else if got[0] != nowStamp || got[1] != earlierStamp {
		t.Errorf("inv-001 runs out of order: %v, want [%s, %s] (newest first)", got, nowStamp, earlierStamp)
	}
	if got := runsByInv["inv-002"]; len(got) != 1 || got[0] != nowStamp {
		t.Errorf("inv-002 runs = %v, want [%s]", got, nowStamp)
	}
}

func TestDiscoverer_List_PopulatesManifestStats(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)

	now := time.Now().UTC()
	uploadInventoryAt(ctx, t, client, bucket, "synthetic-prod", "inventory-data/", "inv-001", now, 100, 9999)

	d := s3disco.New(client, bucket, "inventory-data/")
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(List) = %d, want 1", len(got))
	}
	entry := got[0]
	if entry.FileCount == 0 {
		t.Errorf("FileCount = 0, want > 0 (manifest should reference at least one data file)")
	}
	if entry.TotalBytes == 0 {
		t.Errorf("TotalBytes = 0, want > 0 (manifest data files should report compressed size)")
	}
	if entry.CreationTimestamp == "" {
		t.Errorf("CreationTimestamp = empty, want manifest creation timestamp")
	}
}

func TestDiscoverer_List_SkipsInvalidNames(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)

	now := time.Now().UTC()
	prefix := "inventory-data/"
	uploadInventoryAt(ctx, t, client, bucket, "synthetic-prod", prefix, "inv-001", now, 50, 11)
	stamp := now.Format("2006-01-02T15-04Z")
	for _, key := range []string{
		prefix + "Bad_Src/inv-001/" + stamp + "/manifest.json",
		prefix + "synthetic-prod/inv[alert(1)]/" + stamp + "/manifest.json",
	} {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("{}"),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	got, err := s3disco.New(client, bucket, prefix).List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].SourceBucket != "synthetic-prod" || got[0].Name != "inv-001" {
		t.Errorf("List = %+v, want only synthetic-prod/inv-001", got)
	}
}

func TestDiscoverer_Find(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)

	now := time.Now().UTC()
	// The seeder always names inventories inv-NNN where NNN = index.
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-001", now, 50, 7)
	d := s3disco.New(client, bucket, "inv/")

	// Empty run = "give me the latest".
	got, err := d.Find(ctx, "src-a", "inv-001", "")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	want := now.Format("2006-01-02T15-04Z")
	if got.Run != want {
		t.Errorf("Run = %q, want %q", got.Run, want)
	}

	// Exact-run lookup also works.
	got, err = d.Find(ctx, "src-a", "inv-001", want)
	if err != nil {
		t.Fatalf("Find exact run: %v", err)
	}
	if got.Run != want {
		t.Errorf("exact Run = %q, want %q", got.Run, want)
	}

	if _, err := d.Find(ctx, "", "inv-001", ""); err == nil {
		t.Error("Find with empty src should error")
	}
	if _, err := d.Find(ctx, "src-a", "inv-001", "2099-01-01T00-00Z"); !errors.Is(err, inventory.ErrRunNotFound) {
		t.Errorf("Find with unknown run: err = %v, want ErrRunNotFound", err)
	}
	if _, err := d.Find(ctx, "src-a", "inv-001", "data"); !errors.Is(err, inventory.ErrRunNotFound) {
		t.Errorf("Find with non-run folder: err = %v, want ErrRunNotFound", err)
	}
}

func TestDiscoverer_EmptyBucket(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)

	d := s3disco.New(client, bucket, "inventory-data/")
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(List) = %d on empty bucket, want 0", len(got))
	}
}

// uploadInventoryAt drives the seeder for a specific (src, invID, stamp).
// The seeder package's run-stamp is "now" by default; we use the lower-level
// helper directly so each upload uses a controlled timestamp.
func uploadInventoryAt(ctx context.Context, t *testing.T, client *s3.Client, bucket, srcBucket, prefix, invID string, stamp time.Time, objects int, seed int64) {
	t.Helper()
	// seeder.uploadInventoryToS3 derives the inv-id from index, so we pick
	// an index that produces our desired id. inv-001 → index=1, inv-002 →
	// index=2, etc.
	index := indexFromID(t, invID)
	_, err := seeder.UploadInventory(ctx, client, seeder.Config{
		Target:  seeder.TargetS3,
		Objects: objects,
		Preset:  "small",
		Seed:    seed,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{
		Bucket:    bucket,
		Prefix:    prefix,
		SrcBucket: srcBucket,
	}, index, seed, stamp)
	if err != nil {
		t.Fatalf("upload inventory %s: %v", invID, err)
	}
}

func indexFromID(t *testing.T, id string) int {
	t.Helper()
	switch id {
	case "inv-001":
		return 1
	case "inv-002":
		return 2
	}
	t.Fatalf("unsupported test inv-id %q", id)

	return 0
}

// countingHTTP counts object GETs (GETs that are not listings).
type countingHTTP struct {
	inner s3.HTTPClient
	gets  atomic.Int64
}

func (c *countingHTTP) Do(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && !r.URL.Query().Has("list-type") {
		c.gets.Add(1)
	}

	return c.inner.Do(r) //nolint:wrapcheck // a transport passes the inner client's result through
}

// TestDiscoverer_ManifestsReadOnce: a run's manifest is read once; later
// Lists and Finds of that run reuse it and return the same entries.
func TestDiscoverer_ManifestsReadOnce(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)
	now := time.Now().UTC()
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-001", now.Add(-time.Hour), 50, 1)
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-001", now, 50, 2)
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-002", now, 50, 3)

	counter := &countingHTTP{}
	counted := s3.New(client.Options(), func(o *s3.Options) {
		counter.inner = o.HTTPClient
		o.HTTPClient = counter
	})
	d := s3disco.New(counted, bucket, "inv/")
	first, err := d.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := counter.gets.Load(); n != 3 {
		t.Fatalf("first List read %d manifests, want 3", n)
	}
	second, err := d.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := counter.gets.Load(); n != 3 {
		t.Fatalf("second List read %d more manifests, want 0", n-3)
	}
	if diff := cmp.Diff(first, second); diff != "" {
		t.Fatalf("second List differs (-first +second):\n%s", diff)
	}
	for _, inv := range first {
		got, err := d.Find(ctx, inv.SourceBucket, inv.Name, inv.Run)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(inv, got); diff != "" {
			t.Errorf("Find %s differs from List (-list +find):\n%s", inv.Run, diff)
		}
	}
	if n := counter.gets.Load(); n != 3 {
		t.Fatalf("Finds of listed runs read %d more manifests, want 0", n-3)
	}
}

// TestDiscoverer_ListIndexes: an index source lists committed runs with
// their index size, and skips a run still uploading (files, no
// manifest).
func TestDiscoverer_ListIndexes(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)
	put := func(key, body string) {
		t.Helper()
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	put("idx/src-bucket/daily/2026-10-24T01-00Z/core_stats.bin", "abc")
	put("idx/src-bucket/daily/2026-10-24T01-00Z/manifest.json",
		`{"version":1,"created_at":"2026-10-24T02:00:00Z","node_count":3,"files":{"core_stats.bin":{"size":300,"checksum":"x"},"mph.bin":{"size":40,"checksum":"y"}}}`)
	put("idx/src-bucket/daily/2026-10-25T01-00Z/core_stats.bin", "uploading")

	d := s3disco.New(client, bucket, "idx/").ListIndexes()
	got, err := d.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List = %+v, want only the committed run", got)
	}
	if e := got[0]; e.Run != "2026-10-24T01-00Z" || e.FileFormat != "index" || e.TotalBytes != 340 || e.FileCount != 2 {
		t.Errorf("entry = %+v, want run 2026-10-24T01-00Z, format index, 340 bytes in 2 files", e)
	}
	if _, err := d.Find(ctx, "src-bucket", "daily", "2026-10-25T01-00Z"); !errors.Is(err, inventory.ErrRunNotFound) {
		t.Errorf("Find of the uploading run err = %v, want ErrRunNotFound", err)
	}
}

// TestDiscoverer_ListIndexes_SeesRepublishedRun: republishing a run
// rewrites its manifest at the same key, and deletes it while the new
// files upload. Listings and Find show the current manifest, and hide
// the run while it has none.
func TestDiscoverer_ListIndexes_SeesRepublishedRun(t *testing.T) {
	client := s3test.RawClient(t)
	ctx := t.Context()
	bucket := s3test.Bucket(t, client)
	const key = "idx/src-bucket/daily/2026-10-24T01-00Z/manifest.json"
	putManifest := func(size int) {
		t.Helper()
		body := `{"version":1,"created_at":"2026-10-24T02:00:00Z","node_count":3,"files":{"core_stats.bin":{"size":` +
			strconv.Itoa(size) + `,"checksum":"x"}}}`
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	putManifest(300)
	d := s3disco.New(client, bucket, "idx/").ListIndexes()
	if got, err := d.List(ctx); err != nil || len(got) != 1 || got[0].TotalBytes != 300 {
		t.Fatalf("List = %+v, %v; want one run of 300 bytes", got, err)
	}

	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Find(ctx, "src-bucket", "daily", "2026-10-24T01-00Z"); !errors.Is(err, inventory.ErrRunNotFound) {
		t.Errorf("Find while unpublished err = %v, want ErrRunNotFound", err)
	}

	putManifest(900)
	got, err := d.List(ctx)
	if err != nil || len(got) != 1 || got[0].TotalBytes != 900 {
		t.Fatalf("List after republish = %+v, %v; want one run of 900 bytes", got, err)
	}
	found, err := d.Find(ctx, "src-bucket", "daily", "2026-10-24T01-00Z")
	if err != nil || found.TotalBytes != 900 {
		t.Errorf("Find after republish = %+v, %v; want 900 bytes", found, err)
	}
}
