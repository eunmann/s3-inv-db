package seeder

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/miniotest"
	"github.com/rs/zerolog"
)

func newMinIOS3Client(t *testing.T) *s3.Client {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		t.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	c, err := newS3Client(context.Background())
	if err != nil {
		t.Fatalf("newS3Client: %v", err)
	}

	return c
}

func TestUploadInventory_RoundTrip(t *testing.T) {
	client := newMinIOS3Client(t)
	bucket := miniotest.Bucket(t, client)
	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"
	stamp := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	objects := 50

	info, err := UploadInventory(context.Background(), client, Config{
		Target: TargetS3, Objects: objects, Preset: "small", Seed: 7,
		Logger: zerolog.Nop(),
	}, S3Config{
		Bucket: bucket, Prefix: prefix, SrcBucket: srcBucket,
	}, 1, 7, stamp)
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}
	if info.ID != "inv-001" {
		t.Errorf("InventoryInfo.ID = %q, want inv-001", info.ID)
	}
	wantPath := "s3://" + bucket + "/inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.json"
	if info.Path != wantPath {
		t.Errorf("InventoryInfo.Path = %q, want %q", info.Path, wantPath)
	}

	manifestKey := "inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.json"
	checksumKey := "inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.checksum"
	for _, key := range []string{manifestKey, checksumKey} {
		_, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Errorf("HeadObject %s: %v", key, err)
		}
	}

	manifest := getManifest(t, client, bucket, manifestKey)
	if manifest["sourceBucket"] != srcBucket {
		t.Errorf("manifest sourceBucket = %v, want %s", manifest["sourceBucket"], srcBucket)
	}
	if manifest["fileFormat"] != "CSV" {
		t.Errorf("manifest fileFormat = %v, want CSV", manifest["fileFormat"])
	}
	files, ok := manifest["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("manifest files = %v, want one entry", manifest["files"])
	}
	dataEntry, ok := files[0].(map[string]any)
	if !ok {
		t.Fatalf("manifest file entry not a map: %v", files[0])
	}
	dataKey, _ := dataEntry["key"].(string)
	if got := getCSVGzObjectCount(t, client, bucket, dataKey); got != objects {
		t.Errorf("data file row count = %d, want %d", got, objects)
	}
}

func TestUploadInventory_DataFileURLPlaced(t *testing.T) {
	client := newMinIOS3Client(t)
	bucket := miniotest.Bucket(t, client)
	stamp := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	_, err := UploadInventory(context.Background(), client, Config{
		Target: TargetS3, Objects: 10, Preset: "small", Seed: 1,
		Logger: zerolog.Nop(),
	}, S3Config{Bucket: bucket, SrcBucket: "src"}, 1, 1, stamp)
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}

	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("src/inv-001/data/"),
	})
	count := 0
	for pages.HasMorePages() {
		page, err := pages.NextPage(context.Background())
		if err != nil {
			t.Fatalf("list data dir: %v", err)
		}
		count += len(page.Contents)
	}
	if count != 1 {
		t.Errorf("data dir holds %d objects, want exactly 1", count)
	}
}

func getManifest(t *testing.T, c *s3.Client, bucket, key string) map[string]any {
	t.Helper()
	resp, err := c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject manifest: %v", err)
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	return m
}

func getCSVGzObjectCount(t *testing.T, c *s3.Client, bucket, key string) int {
	t.Helper()
	resp, err := c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject data: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read data body: %v", err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()
	rows, err := csv.NewReader(gr).ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}

	return len(rows)
}
