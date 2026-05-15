package s3fetch_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/miniotest"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

func putObject(t *testing.T, c *s3.Client, bucket, key string, body []byte) {
	t.Helper()
	_, err := c.PutObject(context.Background(), &s3.PutObjectInput{
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
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	putObject(t, c.Raw(), bucket, "manifest.json", []byte(validManifestJSON))

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
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())

	_, err := c.FetchManifest(context.Background(), bucket, "absent.json")
	if err == nil {
		t.Fatal("FetchManifest on missing object returned nil error")
	}
	if !strings.Contains(err.Error(), bucket) {
		t.Errorf("error should mention bucket %q, got: %v", bucket, err)
	}
}

func TestFetchManifest_MalformedJSON(t *testing.T) {
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	putObject(t, c.Raw(), bucket, "manifest.json", []byte("not valid json"))

	_, err := c.FetchManifest(context.Background(), bucket, "manifest.json")
	if err == nil {
		t.Fatal("FetchManifest on garbage body returned nil error")
	}
}

func TestStreamObject_RoundTrip(t *testing.T) {
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	body := []byte("hello s3fetch")
	putObject(t, c.Raw(), bucket, "blob.bin", body)

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
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())

	_, err := c.StreamObject(context.Background(), bucket, "absent.bin")
	if err == nil {
		t.Fatal("StreamObject on missing object returned nil error")
	}
}

func TestDownloadObject_RoundTrip(t *testing.T) {
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	body := bytes.Repeat([]byte("xyzzy"), 4096)
	putObject(t, c.Raw(), bucket, "blob.bin", body)

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
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())

	_, _, err := c.DownloadObject(context.Background(), bucket, "absent.bin")
	if err == nil {
		t.Fatal("DownloadObject on missing object returned nil error")
	}
}

func TestDownloadToFile_RoundTrip(t *testing.T) {
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	body := bytes.Repeat([]byte("payload-"), 2048)
	putObject(t, c.Raw(), bucket, "blob.bin", body)

	dest := filepath.Join(t.TempDir(), "out.bin")
	d := s3fetch.NewDownloader(c.Raw(), s3fetch.DefaultDownloaderConfig())
	result, err := d.DownloadToFile(context.Background(), bucket, "blob.bin", dest)
	if err != nil {
		t.Fatalf("DownloadToFile: %v", err)
	}
	if result.BytesDownloaded != int64(len(body)) {
		t.Errorf("BytesDownloaded = %d, want %d", result.BytesDownloaded, len(body))
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("dest contents differ; len=%d want=%d", len(got), len(body))
	}
}

func TestDownloadToFile_MissingObjectRemovesPartial(t *testing.T) {
	c := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, c.Raw())
	dest := filepath.Join(t.TempDir(), "out.bin")

	d := s3fetch.NewDownloader(c.Raw(), s3fetch.DefaultDownloaderConfig())
	_, err := d.DownloadToFile(context.Background(), bucket, "absent.bin", dest)
	if err == nil {
		t.Fatal("DownloadToFile on missing object returned nil error")
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Errorf("dest file present after failed download: stat err = %v", statErr)
	}
}

func TestDownloader_Config(t *testing.T) {
	c := miniotest.FetchClient(t)
	want := s3fetch.DownloaderConfig{Concurrency: 7, PartSize: 1024, BufferPoolSize: 14}
	d := s3fetch.NewDownloader(c.Raw(), want)
	if got := d.Config(); got != want {
		t.Errorf("Config = %+v, want %+v", got, want)
	}
}

func TestNewClient_ReadsEnvEndpoint(t *testing.T) {
	c, err := s3fetch.NewClient(context.Background())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c.Raw() == nil {
		t.Fatal("Raw() returned nil")
	}
	bucket := miniotest.Bucket(t, c.Raw())
	putObject(t, c.Raw(), bucket, "manifest.json", []byte(validManifestJSON))
	if _, err := c.FetchManifest(context.Background(), bucket, "manifest.json"); err != nil {
		t.Errorf("env-configured client failed FetchManifest: %v", err)
	}
}
