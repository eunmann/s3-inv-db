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

// loadMinIOConfig builds the aws.Config used by both clients: us-east-1
// region with the docker-compose MinIO static credentials. Fails the
// test loudly if AWS_ENDPOINT_URL_S3 is missing so a misconfigured
// runner can't silently skip.
func loadMinIOConfig(tb testing.TB) (aws.Config, string) {
	tb.Helper()
	endpoint := os.Getenv(s3fetch.EnvEndpointURL)
	if endpoint == "" {
		tb.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		tb.Fatalf("aws config: %v", err)
	}

	return cfg, endpoint
}

// RawClient returns an aws-sdk-go-v2 S3 client wired to the MinIO
// endpoint in AWS_ENDPOINT_URL_S3.
func RawClient(tb testing.TB) *s3.Client {
	tb.Helper()
	cfg, endpoint := loadMinIOConfig(tb)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// FetchClient is the project's *s3fetch.Client wired to MinIO.
func FetchClient(tb testing.TB) *s3fetch.Client {
	tb.Helper()
	cfg, _ := loadMinIOConfig(tb)

	return s3fetch.NewClientWithConfig(cfg)
}

// Bucket creates a uniquely-named bucket and registers cleanup that
// empties and deletes it on test exit. Names are derived from tb.Name()
// so the bucket is identifiable mid-test, with a timestamp suffix to
// avoid collisions on re-runs.
func Bucket(tb testing.TB, c *s3.Client) string {
	tb.Helper()
	name := bucketName(tb.Name())
	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		tb.Fatalf("CreateBucket %s: %v", name, err)
	}
	tb.Cleanup(func() { empty(tb, c, name) })

	return name
}

func empty(tb testing.TB, c *s3.Client, bucket string) {
	tb.Helper()
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			tb.Logf("list for cleanup: %v", err)

			return
		}
		for _, obj := range page.Contents {
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key})
		}
	}
	_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

var bucketSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// maxBucketSlug bounds the sanitised test-name component so the full
// "t-<slug>-<timestamp>" bucket name stays within S3's 63-char limit.
const maxBucketSlug = 40

// bucketName sanitises a Go test name into a valid S3 bucket name:
// lowercase, 3-63 chars, ASCII alphanumeric + dash.
func bucketName(testName string) string {
	clean := bucketSanitizer.ReplaceAllString(strings.ToLower(testName), "-")
	clean = strings.Trim(clean, "-")
	if len(clean) > maxBucketSlug {
		clean = clean[:maxBucketSlug]
	}
	stamp := strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000"), ".", "")

	return "t-" + clean + "-" + stamp
}
