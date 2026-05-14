// Package miniotest provides shared helpers for tests that hit MinIO
// via docker compose's test profile. Every helper creates resources
// scoped to t and registers cleanup on t.Cleanup.
package miniotest

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// RawClient returns an aws-sdk-go-v2 S3 client wired to the MinIO
// endpoint in AWS_ENDPOINT_URL_S3. Fails the test loudly if the env
// is missing so a misconfigured runner can't silently skip.
func RawClient(t *testing.T) *s3.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL_S3")
	if endpoint == "" {
		t.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// FetchClient is the project's *s3fetch.Client wired to MinIO.
func FetchClient(t *testing.T) *s3fetch.Client {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL_S3")
	if endpoint == "" {
		t.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return s3fetch.NewClientWithConfig(cfg)
}

// Bucket creates a uniquely-named bucket and registers cleanup that
// empties and deletes it on test exit. Names are derived from t.Name()
// so the bucket is identifiable mid-test, with a timestamp suffix to
// avoid collisions on re-runs.
func Bucket(t *testing.T, c *s3.Client) string {
	t.Helper()
	name := bucketName(t.Name())
	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket %s: %v", name, err)
	}
	t.Cleanup(func() { empty(t, c, name) })
	return name
}

func empty(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("list for cleanup: %v", err)
			return
		}
		for _, obj := range page.Contents {
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
		}
	}
	_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

var bucketSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// bucketName sanitises a Go test name into a valid S3 bucket name:
// lowercase, 3-63 chars, ASCII alphanumeric + dash.
func bucketName(testName string) string {
	clean := bucketSanitizer.ReplaceAllString(strings.ToLower(testName), "-")
	clean = strings.Trim(clean, "-")
	if len(clean) > 40 {
		clean = clean[:40]
	}
	stamp := strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000"), ".", "")
	return "t-" + clean + "-" + stamp
}
