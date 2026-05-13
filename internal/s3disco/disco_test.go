package s3disco

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/rs/zerolog"
)

// minIOFromEnv builds an S3 client pointing at MinIO. If AWS_ENDPOINT_URL_S3
// is unset (i.e., we're not in the dev compose stack), it skips the test.
func minIOFromEnv(t *testing.T) *s3.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL_S3")
	if endpoint == "" {
		t.Skip("AWS_ENDPOINT_URL_S3 not set; skipping (run inside docker compose dev stack to exercise)")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			envOr("AWS_ACCESS_KEY_ID", "minioadmin"),
			envOr("AWS_SECRET_ACCESS_KEY", "minioadmin"),
			"",
		)),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestDiscoverer_List_AgainstMinIO(t *testing.T) {
	client := minIOFromEnv(t)
	ctx := context.Background()

	// Each test gets its own bucket so it can't see fixtures from other runs.
	bucket := newTestBucket(t, client)

	// Upload two inventories with two timestamp folders each, via the
	// seeder package so we exercise the same code path the CLI uses.
	now := time.Now().UTC()
	earlier := now.Add(-time.Hour)
	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"

	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", earlier, 100, 1001)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", now, 100, 1002)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-002", now, 100, 2001)

	d := New(client, bucket, prefix)
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(List) = %d, want 2; got=%+v", len(got), got)
	}

	byID := map[string]Inventory{}
	for _, e := range got {
		byID[e.InventoryID] = e
	}

	one, ok := byID["inv-001"]
	if !ok {
		t.Fatal("inv-001 missing from List")
	}
	wantStamp := now.Format("2006-01-02T15-04Z")
	if one.LatestRun != wantStamp {
		t.Errorf("inv-001 LatestRun = %q, want %q (the newer of two runs)", one.LatestRun, wantStamp)
	}
	if one.SourceBucket != srcBucket {
		t.Errorf("inv-001 SourceBucket = %q, want %q", one.SourceBucket, srcBucket)
	}
	if one.FileFormat != "CSV" {
		t.Errorf("inv-001 FileFormat = %q, want CSV", one.FileFormat)
	}
	if one.FileCount != 1 {
		t.Errorf("inv-001 FileCount = %d, want 1", one.FileCount)
	}

	two, ok := byID["inv-002"]
	if !ok {
		t.Fatal("inv-002 missing from List")
	}
	if two.LatestRun != wantStamp {
		t.Errorf("inv-002 LatestRun = %q, want %q", two.LatestRun, wantStamp)
	}
}

func TestDiscoverer_Find(t *testing.T) {
	client := minIOFromEnv(t)
	ctx := context.Background()
	bucket := newTestBucket(t, client)

	now := time.Now().UTC()
	// The seeder always names inventories inv-NNN where NNN = index.
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-001", now, 50, 7)
	d := New(client, bucket, "inv/")

	got, err := d.Find(ctx, "src-a", "inv-001")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.LatestRun != now.Format("2006-01-02T15-04Z") {
		t.Errorf("LatestRun = %q, want %q", got.LatestRun, now.Format("2006-01-02T15-04Z"))
	}

	if _, err := d.Find(ctx, "", "inv-001"); err == nil {
		t.Error("Find with empty src should error")
	}
}

func TestDiscoverer_EmptyBucket(t *testing.T) {
	client := minIOFromEnv(t)
	ctx := context.Background()
	bucket := newTestBucket(t, client)

	d := New(client, bucket, "inventory-data/")
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

// newTestBucket creates a unique bucket and registers cleanup. Bucket
// names must be lowercase, 3-63 chars; we use a timestamp + random suffix.
func newTestBucket(t *testing.T, client *s3.Client) string {
	t.Helper()
	ctx := context.Background()
	name := "t-" + time.Now().Format("20060102150405.000000")
	// strip the dot from the fractional seconds — bucket names disallow it
	clean := make([]byte, 0, len(name))
	for _, b := range []byte(name) {
		if b == '.' {
			continue
		}
		clean = append(clean, b)
	}
	bucket := string(clean)
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	t.Cleanup(func() {
		emptyAndDeleteBucket(t, client, bucket)
	})
	return bucket
}

func emptyAndDeleteBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("list for cleanup: %v", err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key}); err != nil {
				t.Logf("delete %s: %v", aws.ToString(obj.Key), err)
			}
		}
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("delete bucket: %v", err)
	}
}
