package s3fetch

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newMinIOClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		t.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return NewClientWithConfig(cfg)
}

func newTestBucket(t *testing.T, c *Client) string {
	t.Helper()
	ctx := context.Background()
	name := "t-s3fetch-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000"), ".", "")
	if _, err := c.Raw().CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket %s: %v", name, err)
	}
	t.Cleanup(func() { emptyBucket(t, c, name) })
	return name
}

func emptyBucket(t *testing.T, c *Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(c.Raw(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("list for cleanup: %v", err)
			return
		}
		for _, obj := range page.Contents {
			_, _ = c.Raw().DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
		}
	}
	_, _ = c.Raw().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

func putObject(t *testing.T, c *Client, bucket, key string, body []byte) {
	t.Helper()
	_, err := c.Raw().PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject s3://%s/%s: %v", bucket, key, err)
	}
}

const validManifestJSON = `{
  "sourceBucket": "src-bucket",
  "destinationBucket": "arn:aws:s3:::dst-bucket",
  "version": "2016-11-30",
  "fileFormat": "CSV",
  "fileSchema": "Bucket, Key, Size, LastModifiedDate",
  "files": [{"key": "data/file1.csv.gz", "size": 1234, "MD5checksum": "abc"}]
}`

func TestFetchManifest_RoundTrip(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)
	putObject(t, c, bucket, "manifest.json", []byte(validManifestJSON))

	got, err := c.FetchManifest(context.Background(), bucket, "manifest.json")
	if err != nil {
		t.Fatalf("FetchManifest: %v", err)
	}
	if got.SourceBucket != "src-bucket" {
		t.Errorf("SourceBucket = %q, want %q", got.SourceBucket, "src-bucket")
	}
	if got.FileFormat != "CSV" {
		t.Errorf("FileFormat = %q, want %q", got.FileFormat, "CSV")
	}
	if len(got.Files) != 1 || got.Files[0].Key != "data/file1.csv.gz" {
		t.Errorf("Files = %+v, want one entry with key data/file1.csv.gz", got.Files)
	}
}

func TestFetchManifest_MissingObject(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)

	_, err := c.FetchManifest(context.Background(), bucket, "absent.json")
	if err == nil {
		t.Fatal("FetchManifest on missing object returned nil error")
	}
	if !strings.Contains(err.Error(), bucket) {
		t.Errorf("error should mention bucket %q, got: %v", bucket, err)
	}
}

func TestFetchManifest_MalformedJSON(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)
	putObject(t, c, bucket, "manifest.json", []byte("not valid json"))

	_, err := c.FetchManifest(context.Background(), bucket, "manifest.json")
	if err == nil {
		t.Fatal("FetchManifest on garbage body returned nil error")
	}
}

func TestStreamObject_RoundTrip(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)
	body := []byte("hello s3fetch")
	putObject(t, c, bucket, "blob.bin", body)

	r, err := c.StreamObject(context.Background(), bucket, "blob.bin")
	if err != nil {
		t.Fatalf("StreamObject: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body = %q, want %q", got, body)
	}
}

func TestStreamObject_Missing(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)

	_, err := c.StreamObject(context.Background(), bucket, "absent.bin")
	if err == nil {
		t.Fatal("StreamObject on missing object returned nil error")
	}
}

func TestDownloadObject_RoundTrip(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)
	body := bytes.Repeat([]byte("xyzzy"), 4096)
	putObject(t, c, bucket, "blob.bin", body)

	r, result, err := c.DownloadObject(context.Background(), bucket, "blob.bin")
	if err != nil {
		t.Fatalf("DownloadObject: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body length = %d, want %d", len(got), len(body))
	}
	if result == nil {
		t.Fatal("DownloadResult is nil")
	}
	if result.BytesDownloaded != int64(len(body)) {
		t.Errorf("BytesDownloaded = %d, want %d", result.BytesDownloaded, len(body))
	}
}

func TestDownloadObject_Missing(t *testing.T) {
	c := newMinIOClient(t)
	bucket := newTestBucket(t, c)

	_, _, err := c.DownloadObject(context.Background(), bucket, "absent.bin")
	if err == nil {
		t.Fatal("DownloadObject on missing object returned nil error")
	}
}

func TestNewClient_ReadsEnvEndpoint(t *testing.T) {
	c, err := NewClient(context.Background())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c.Raw() == nil {
		t.Fatal("Raw() returned nil")
	}
	bucket := newTestBucket(t, c)
	putObject(t, c, bucket, "manifest.json", []byte(validManifestJSON))
	if _, err := c.FetchManifest(context.Background(), bucket, "manifest.json"); err != nil {
		t.Errorf("env-configured client failed FetchManifest: %v", err)
	}
}
