package loader_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

func newClient(t *testing.T) (*s3fetch.Client, *s3.Client) {
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
	fc := s3fetch.NewClientWithConfig(cfg)
	return fc, fc.Raw()
}

func mkBucket(t *testing.T, c *s3.Client) string {
	t.Helper()
	name := "t-loader-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000"), ".", "")
	if _, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(name)})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return
			}
			for _, obj := range page.Contents {
				_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(name), Key: obj.Key})
			}
		}
		_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	})
	return name
}

func TestBuildWith_BuildsIndexFromSeededManifest(t *testing.T) {
	fc, raw := newClient(t)
	bucket := mkBucket(t, raw)

	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"
	stamp := time.Now().UTC().Truncate(time.Minute)

	info, err := seeder.UploadInventory(context.Background(), raw, seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 200,
		Preset:  "small",
		Seed:    42,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{
		Bucket:    bucket,
		Prefix:    prefix,
		SrcBucket: srcBucket,
	}, 1, 42, stamp)
	if err != nil {
		t.Fatalf("seeder.UploadInventory: %v", err)
	}

	cacheRoot := t.TempDir()
	l := loader.New(cacheRoot, fc)

	run := stamp.Format("2006-01-02T15-04Z")
	var stages []string
	outDir, err := l.BuildWith(context.Background(), srcBucket, info.ID, run, info.Path,
		func(stage string, _, _ int64) { stages = append(stages, stage) },
	)
	if err != nil {
		t.Fatalf("BuildWith: %v", err)
	}
	if outDir != l.CacheDirFor(srcBucket, info.ID, run) {
		t.Errorf("outDir = %q, want %q", outDir, l.CacheDirFor(srcBucket, info.ID, run))
	}
	for _, want := range []string{"preparing", "done"} {
		found := false
		for _, s := range stages {
			if s == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("stages = %v, missing %q", stages, want)
		}
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("open built index: %v", err)
	}
	defer idx.Close()
	if idx.Count() == 0 {
		t.Error("index has zero prefixes")
	}
	pos, ok := idx.Lookup("")
	if !ok {
		t.Fatal("root prefix missing")
	}
	stats := idx.Stats(pos)
	if stats.ObjectCount == 0 {
		t.Error("root prefix has zero objects")
	}
}

func TestBuild_FailsOnMissingManifest(t *testing.T) {
	fc, raw := newClient(t)
	bucket := mkBucket(t, raw)

	l := loader.New(t.TempDir(), fc)
	bogus := fmt.Sprintf("s3://%s/does/not/exist/manifest.json", bucket)
	_, err := l.Build(context.Background(), "src", "inv", "run", bogus)
	if err == nil {
		t.Fatal("Build with missing manifest returned nil error")
	}
	if !strings.Contains(err.Error(), "pipeline") {
		t.Logf("build error (informational): %v", err)
	}
	cacheDir := l.CacheDirFor("src", "inv", "run")
	if _, statErr := filepath.Glob(filepath.Join(cacheDir, "*")); statErr != nil {
		t.Logf("partial cache stat: %v", statErr)
	}
}
